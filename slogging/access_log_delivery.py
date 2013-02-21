# Copyright (c) 2010-2011 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import collections
import datetime
from uuid import uuid4
import Queue
from urllib import unquote
import os
import cPickle
import cStringIO
import functools
import random
import errno

from swift.common.daemon import Daemon
from swift.common.utils import get_logger, TRUE_VALUES, split_path
from swift.common.exceptions import LockTimeout, ChunkReadTimeout
from slogging.log_common import LogProcessorCommon, multiprocess_collate, \
                                   BadFileDownload
from slogging.file_buffer import FileBuffer


month_map = '_ Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
MEMOIZE_KEY_LIMIT = 10000
MEMOIZE_FLUSH_RATE = 0.25


def make_clf_from_parts(parts):
    format = '%(client_ip)s - - [%(day)s/%(month)s/%(year)s:%(hour)s:' \
             '%(minute)s:%(second)s %(tz)s] "%(method)s %(request)s ' \
             '%(http_version)s" %(code)s %(bytes_out)s "%(referrer)s" ' \
             '"%(user_agent)s"'
    try:
        return format % parts
    except KeyError:
        return None


def memoize(func):
    cache = {}

    @functools.wraps(func)
    def wrapped(*args):
        key = tuple(args)
        if key in cache:
            return cache[key]
        result = func(*args)
        len_cache = len(cache)
        if len_cache > MEMOIZE_KEY_LIMIT:
            cache_keys = cache.keys()
            for _unused in xrange(int(len_cache * MEMOIZE_FLUSH_RATE)):
                index_to_delete = random.randrange(0, len(cache_keys))
                key_to_delete = cache_keys.pop(index_to_delete)
                del cache[key_to_delete]
        cache[key] = result
        return result
    return wrapped


class AccessLogDelivery(LogProcessorCommon):

    def __init__(self, conf, logger):
        super(AccessLogDelivery, self).__init__(conf, logger,
                                                'access-log-delivery')
        self.frequency = int(conf.get('frequency', '3600'))
        self.metadata_key = conf.get('metadata_key',
                                'x-container-meta-access-log-delivery').lower()
        self.server_name = conf.get('server_name', 'proxy-server')
        self.working_dir = conf.get('working_dir', '/tmp/swift').rstrip('/')
        buffer_limit = conf.get('buffer_limit', '10485760')
        self.file_buffer = FileBuffer(buffer_limit, logger)
        self.hidden_ips = [x.strip() for x in
                            conf.get('hidden_ips', '').split(',') if x.strip()]
        self.source_account = conf['log_source_account']
        self.source_container = conf.get('log_source_container_name',
                                         'log_data')

    def get_logs_to_process(self, already_processed_files):
        lookback_start, lookback_end = self.calculate_lookback()
        logs_to_process = self.get_container_listing(
                                                self.source_account,
                                                self.source_container,
                                                lookback_start,
                                                lookback_end,
                                                already_processed_files)
        logs_to_process = [(self.source_account, self.source_container, x)
                            for x in logs_to_process]
        self.logger.info(_('loaded %d files to process') %
                         len(logs_to_process))
        return logs_to_process

    def process_one_file(self, account, container, object_name):
        files_to_upload = set()
        try:
            year, month, day, hour, _unused = object_name.split('/', 4)
        except ValueError:
            self.logger.info(_('Odd object name: %s. Skipping' % object_name))
            return
        filename_pattern = '%s/%%s/%%s/%s/%s/%s/%s' % (self.working_dir, year,
                                                   month, day, hour)
        self.logger.debug(_('Processing %s' % object_name))
        # get an iter of the object data
        compressed = object_name.endswith('.gz')
        stream = self.get_object_data(account, container, object_name,
                                      compressed=compressed)
        buff = collections.defaultdict(list)
        for line in stream:
            clf, account, container = self.convert_log_line(line)
            if not clf or not account or not container:
                # bad log line
                continue
            if self.get_container_save_log_flag(account, container):
                filename = filename_pattern % (account, container)
                self.file_buffer.write(filename, clf)
                files_to_upload.add(filename)
        self.file_buffer.flush()
        return files_to_upload

    @memoize
    def get_container_save_log_flag(self, account, container):
        key = 'save-access-logs-%s-%s' % (account, container)
        flag = self.memcache.get(key)
        if flag is None:
            metadata = self.internal_proxy.get_container_metadata(account,
                                                                  container)
            val = metadata.get(self.metadata_key, '')
            flag = val.lower() in TRUE_VALUES
            self.memcache.set(key, flag, timeout=self.frequency)
        return flag

    def convert_log_line(self, raw_log):
        parts = self.log_line_parser(raw_log)
        if parts == {}:
            return None, None, None
        return (make_clf_from_parts(parts),
                parts.get('account'),
                parts.get('container_name'))

    def log_line_parser(self, raw_log):
        '''given a raw access log line, return a dict of the good parts'''
        d = {}
        try:
            log_arr = raw_log[16:].split(' ')
            if len(log_arr) > 18:
                log_source = log_arr[18]
                if log_source != '-':
                    # internal proxy log
                    return {}
            (unused,
            server,
            client_ip,
            lb_ip,
            timestamp,
            method,
            request,
            http_version,
            code,
            referrer,
            user_agent,
            auth_token,
            bytes_in,
            bytes_out,
            etag,
            trans_id,
            headers,
            processing_time) = (unquote(x) for x in log_arr[:18])
        except ValueError:
            self.logger.debug(_('Bad line data: %s') % repr(raw_log))
            return {}
        if server != self.server_name:
            # incorrect server name in log line
            self.logger.debug(_('Bad server name: found "%(found)s" ' \
                    'expected "%(expected)s"') %
                    {'found': server, 'expected': self.server_name})
            return {}
        try:
            (version, account, container_name, object_name) = \
                split_path(request, 2, 4, True)
        except ValueError, e:
            self.logger.debug(_('Invalid path: %(error)s from data: %(log)s') %
            {'error': e, 'log': repr(raw_log)})
            return {}
        if container_name is not None:
            container_name = container_name.split('?', 1)[0]
        if object_name is not None:
            object_name = object_name.split('?', 1)[0]
        account = account.split('?', 1)[0]
        if client_ip in self.hidden_ips:
            client_ip = '0.0.0.0'
        d['client_ip'] = client_ip
        d['lb_ip'] = lb_ip
        d['method'] = method
        d['request'] = request
        d['http_version'] = http_version
        d['code'] = code
        d['referrer'] = referrer
        d['user_agent'] = user_agent
        d['auth_token'] = auth_token
        d['bytes_in'] = bytes_in
        d['bytes_out'] = bytes_out
        d['etag'] = etag
        d['trans_id'] = trans_id
        d['processing_time'] = processing_time
        day, month, year, hour, minute, second = timestamp.split('/')
        d['day'] = day
        month = ('%02s' % month_map.index(month)).replace(' ', '0')
        d['month'] = month
        d['year'] = year
        d['hour'] = hour
        d['minute'] = minute
        d['second'] = second
        d['tz'] = '+0000'
        d['account'] = account
        d['container_name'] = container_name
        d['object_name'] = object_name
        d['bytes_out'] = int(d['bytes_out'].replace('-', '0'))
        d['bytes_in'] = int(d['bytes_in'].replace('-', '0'))
        d['code'] = int(d['code'])
        return d


class AccessLogDeliveryDaemon(Daemon):
    """
    Processes access (proxy) logs to split them up by account and deliver the
    split logs to their respective accounts.
    """

    def __init__(self, c):
        self.conf = c
        super(AccessLogDeliveryDaemon, self).__init__(c)
        self.logger = get_logger(c, log_route='access-log-delivery')
        self.log_processor = AccessLogDelivery(c, self.logger)
        self.log_delivery_account = c['swift_account']
        self.log_delivery_container = c.get('container_name',
                                            'access_log_delivery_data')
        self.target_container = c.get('target_container', '.ACCESS_LOGS')
        self.frequency = int(c.get('frequency', '3600'))
        self.worker_count = int(c.get('worker_count', '1'))
        self.working_dir = c.get('working_dir', '/tmp/swift')
        if self.working_dir.endswith('/'):
            self.working_dir = self.working_dir.rstrip('/')

    def run_once(self, *a, **kw):
        self.logger.info(_("Beginning log processing"))
        start = time.time()
        already_processed_files = \
            self.log_processor.load_already_processed_files()
        lookback_hours = kw.get('lookback_hours')
        if lookback_hours:
            self.log_processor.lookback_hours = lookback_hours
        lookback_window = kw.get('lookback_window')
        if lookback_window:
            self.log_processor.lookback_window = lookback_window
        logs_to_process = \
            self.log_processor.get_logs_to_process(already_processed_files)
        if not logs_to_process:
            self.logger.info(_("Log processing done (%0.2f minutes)") %
                        ((time.time() - start) / 60))
            return

        # map
        processor_args = (self.conf, self.logger)
        results = multiprocess_collate(AccessLogDelivery, processor_args,
                                       'process_one_file', logs_to_process,
                                       self.worker_count)

        #reduce
        processed_files = already_processed_files
        files_to_upload = set()
        for item, data in results:
            a, c, o = item
            processed_files.add(o)
            if data:
                files_to_upload.update(data)
        len_working_dir = len(self.working_dir) + 1  # +1 for the trailing '/'
        for filename in files_to_upload:
            target_name = filename[len_working_dir:]
            account, target_name = target_name.split('/', 1)
            some_id = uuid4().hex
            target_name = '%s/%s.log.gz' % (target_name, some_id)
            success = self.log_processor.internal_proxy.upload_file(filename,
                            account,
                            self.target_container,
                            target_name)
            if success:
                os.unlink(filename)
                self.logger.debug('Uploaded %s to account %s' % (filename,
                                                                 account))
            else:
                self.logger.error('Could not upload %s to account %s' % (
                                    filename, account))

        # cleanup
        success = self.log_processor.save_processed_files(processed_files)
        if not success:
            self.logger.error('Error uploading updated processed files log')
        self.logger.info(_("Log processing done (%0.2f minutes)") %
                    ((time.time() - start) / 60))

    def run_forever(self, *a, **kw):
        while True:
            start_time = time.time()
            try:
                self.run_once()
            except Exception:
                self.logger.exception('Run once failed')
            end_time = time.time()
            # don't run more than once every self.frequency seconds
            sleep_time = self.frequency - (end_time - start_time)
            time.sleep(max(0, sleep_time))
