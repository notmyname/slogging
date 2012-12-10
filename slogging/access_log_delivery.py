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

from __future__ import with_statement

import time
import datetime
import Queue
from urllib import unquote
import os
import cPickle
import cStringIO
import functools
import random
import errno
import shutil
import cPickle
import hashlib
import json
import multiprocessing

from swift.common.daemon import Daemon
from swift.common.utils import get_logger, TRUE_VALUES, split_path, lock_file
from swift.common.exceptions import LockTimeout, ChunkReadTimeout
from swift.common.constraints import MAX_OBJECT_NAME_LENGTH
from slogging.log_common import LogProcessorCommon, BadFileDownload
from slogging.file_buffer import FileBuffer
from slogging.utils import calculate_filehash
from slogging.line_config_parser import LineConfigParser


month_map = '_ Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
MEMOIZE_KEY_LIMIT = 10000
MEMOIZE_FLUSH_RATE = 0.25


# logger passed to each process spawned by multiprocessing
# so we have but one logger
global_logger = None


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


class InfoFile(object):
    def __init__(self, filename, fields=None):
        self.filename = filename
        self.fields = 'account container_name year month day hour'.split()

        if not self.filename.endswith('.info'):
            raise ValueError('Unexpected file name')

        if fields is None:
            self.read()
        else:
            for field in self.fields[:2]:
                setattr(self, field, fields[field])
            for field in self.fields[2:]:
                setattr(self, field, int(fields[field]))

    def delete(self):
        os.remove(self.filename)

    def read(self):
        if not os.path.exists(self.filename):
            raise RuntimeError("File '%s' does not exist" % self.filename)

        with open(self.filename, 'r') as f:
            data = json.load(f)
        for field in self.fields:
            setattr(self, field, data[field])

    @property
    def timestamp(self):
        return '%04d/%02d/%02d/%02d' % \
            (self.year, self.month, self.day, self.hour)

    @property
    def upload_file(self):
        return self.filename[:-len('.info')]

    def write(self):
        data = {}
        for field in self.fields:
            data[field] = getattr(self, field)
        dirname = os.path.dirname(self.filename)
        if not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except OSError, err:
                if err.errno != errno.EEXIST:
                    raise
        with lock_file(self.filename, unlink=False) as f:
            json.dump(data, f)


class Stopwatch(object):
    def __init__(self, logger, msg):
        self.logger = logger
        self.msg = msg

    def __enter__(self):
        self.logger.info(self.msg)
        self.start_time = time.time()

    def __exit__(self, type, value, traceback):
        self.logger.info("%s done (%0.1f secs)" %
                         (self.msg, time.time() - self.start_time))


def multiprocess_process(args):
    conf, klass, klass_args, method, method_args = args

    try:
        p = klass(klass_args[0], global_logger)
        method = getattr(p, method)
        method(*method_args)
    except Exception:
        logger = get_logger(conf, 'access-log-delivery')
        logger.exception('Exception in multiprocess_process()')
        return False
    return True


class AccessLogDelivery(LogProcessorCommon):

    def __init__(self, conf, logger=None):
        super(AccessLogDelivery, self).__init__(conf, logger,
                                                'access-log-delivery')
        self.memcache_timeout = int(conf.get('memcache_timeout', '3600'))
        self.metadata_key = \
            conf.get('metadata_key',
                     'x-container-meta-access-log-delivery').lower()
        self.server_name = conf.get('server_name', 'proxy-server')
        self.working_dir = conf.get('working_dir', '/tmp/swift')
        self.output_dir = os.path.join(self.working_dir, 'output')
        self.upload_dir = os.path.join(self.working_dir, 'upload')
        buffer_limit = conf.get('buffer_limit', '10485760')
        self.file_buffer = FileBuffer(buffer_limit, self.logger)
        self.source_account = conf['log_source_account']
        self.source_container = conf.get('log_source_container_name',
                                         'log_data')
        self.target_container = conf.get('target_container', '.ACCESS_LOGS')
        self.worker_count = int(conf.get('worker_count', '1'))
        # 0 means to process all the logs found
        self.logs_to_process_per_run = int(conf.get('logs_to_process_per_run',
                                                    0))
        self.num_delivery_dirs = int(conf.get('num_delivery_dirs', 1000))
        self.error_filters = LineConfigParser.get_regular_expressions(
            conf.get('error_filters_conf'), 'access_log_delivery')
        for conf_tag in ['hidden_ips', 'service_log_sources']:
            setattr(self, conf_tag,
                    [x.strip() for x in conf.get(conf_tag, '').split(',')
                    if x.strip()])
        self.log_line_count = int(conf.get('log_line_count', 250000))
        self.test_mode = conf.get('test_mode', 'false').lower() in TRUE_VALUES
        self.test_container_percentage = int(conf.get(
            'test_container_percentage', 1))
        self.test_target_account = conf.get('test_target_account')
        if self.test_mode and not self.test_target_account:
            raise Exception('test_target_account not set in config')

    def bad_log_line(self, object_name, line):
        for ef in self.error_filters:
            if ef.match(line):
                return False
        filename = os.path.join(self.working_dir, 'error_lines',
                                object_name.replace('/', '_'))
        self.file_buffer.write(filename, line)
        return True

    def get_logs_to_process(self, already_processed_files):
        lookback_start, lookback_end = self.calculate_lookback()
        logs_to_process = self.get_container_listing(self.source_account,
                                                     self.source_container,
                                                     lookback_start,
                                                     lookback_end,
                                                     already_processed_files)
        logs_to_process = [(self.source_account, self.source_container, x)
                           for x in logs_to_process]
        if self.logs_to_process_per_run:
            logs_to_process = logs_to_process[:self.logs_to_process_per_run]
        self.logger.info('loaded %d files to process' % len(logs_to_process))
        return logs_to_process

    def calculate_upload_filename(self, fields):
        fields_to_hash = 'account container_name year month day hour'.split()
        fields = dict(fields)
        for key in 'account container_name'.split():
            fields[key] = fields[key].encode('utf-8')
        md5 = hashlib.md5()
        md5.update(str(len(fields['account'])))
        for field in fields_to_hash:
            md5.update(fields[field])
        hash_ = md5.hexdigest()
        dirname = str(int(hash_, 16) % self.num_delivery_dirs)
        dirname = dirname.zfill(len(str(self.num_delivery_dirs)))
        return os.path.join(self.output_dir, dirname, hash_)

    def process_one_object(self, account, container, object_name):
        try:
            year, month, day, hour, _unused = object_name.split('/', 4)
        except ValueError:
            raise Exception('Odd object name: %s' % (object_name))

        self.logger.info('Processing %s' % object_name)
        # get an iter of the object data
        compressed = object_name.endswith('.gz')
        stream = self.get_object_data(account, container, object_name,
                                      compressed=compressed)
        line_count = 0
        for line in stream:
            line_count += 1
            if line_count % self.log_line_count == 0:
                self.logger.info(
                    'On line %d of %s' % (line_count, object_name))
            try:
                fields = self.log_line_parser(line)
            except Exception:
                if self.bad_log_line(object_name, line):
                    self.logger.exception('Log line parser threw an exception')
                continue
            if not fields.get('account'):
                self.bad_log_line(object_name, line)
                continue
            elif not fields.get('container_name'):
                # we don't deliver lines without a container
                continue
            # we don't deliver service log lines
            if fields['log_source'] in self.service_log_sources:
                continue
            if self.get_container_save_log_flag(fields['account'],
                                                fields['container_name']):
                output_line = make_clf_from_parts(fields)
                if output_line is None:
                    self.bad_log_line(object_name, line)
                    continue
                filename = self.calculate_upload_filename(fields)
                if not self.file_buffer.exists(filename):
                    InfoFile(filename + '.info', fields).write()
                self.file_buffer.write(filename, output_line)
        self.file_buffer.flush()

    @memoize
    def get_container_save_log_flag(self, account, container):
        key = 'save-access-logs-%s-%s' % (
            account.encode('utf-8'), container.encode('utf-8'))
        if self.test_mode:
            hash_ = hashlib.md5(key).hexdigest()
            return int(hash_, 16) % 100 < self.test_container_percentage
        flag = self.memcache.get(key)
        if flag is None:
            metadata = self.internal_proxy.get_container_metadata(account,
                                                                  container)
            val = metadata.get(self.metadata_key, '')
            flag = val.lower() in TRUE_VALUES
            self.memcache.set(key, flag, timeout=self.memcache_timeout)
        return flag

    def log_line_parser(self, raw_log):
        '''given a raw access log line, return a dict of the good parts'''
        try:
            log_source = None
            split_log = raw_log[16:].split(' ')
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
             processing_time) = (unquote(x) for x in split_log[:18])
            if len(split_log) > 18:
                log_source = split_log[18]
        except ValueError:
            self.logger.debug('Bad line data: %s' % repr(raw_log))
            return {}
        if server != self.server_name:
            # incorrect server name in log line
            self.logger.debug('Bad server name: found "%(found)s" '
                              'expected "%(expected)s"' %
                              {'found': server, 'expected': self.server_name})
            return {}
        # request path is double quoted by logging
        # parameter portion of request is single quoted
        request_path = request.split('?', 1)[0]
        try:
            (version, account, container_name, object_name) = \
                split_path(request_path, 2, 4, True)
        except ValueError, e:
            self.logger.debug('Invalid path: %(error)s from data: %(log)s' %
                              {'error': e, 'log': repr(raw_log)})
            return {}
        account = unquote(account)
        if container_name is not None:
            container_name = unquote(container_name)
        if object_name is not None:
            object_name = unquote(object_name)
        if client_ip in self.hidden_ips:
            client_ip = '0.0.0.0'
        d = {}
        d['client_ip'] = client_ip
        d['lb_ip'] = lb_ip
        d['method'] = method
        d['request'] = split_log[6]
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
        try:
            day, month, year, hour, minute, second = timestamp.split('/')
        except ValueError:
            self.logger.debug("Invalid timestamp: '%s'" % (timestamp))
            return {}
        d['day'] = day
        try:
            month = str(month_map.index(month)).zfill(2)
        except ValueError:
            self.logger.debug("Invalid month: '%s'" % (month))
            return {}
        d['month'] = month
        d['year'] = year
        d['hour'] = hour
        d['minute'] = minute
        d['second'] = second
        d['tz'] = '+0000'
        try:
            d['account'] = account.decode('utf-8')
        except ValueError:
            self.logger.debug("Account utf-8 decode error: '%s'" % (account))
            return {}
        if container_name is not None:
            try:
                d['container_name'] = container_name.decode('utf-8')
            except ValueError:
                self.logger.debug("Container utf-8 decode error: '%s'" %
                                  (container_name))
                return {}
        if object_name is not None:
            try:
                d['object_name'] = object_name.decode('utf-8')
            except ValueError:
                self.logger.debug("Object utf-8 decode error: '%s'" %
                                  (object_name))
                return {}
        try:
            d['bytes_out'] = int(d['bytes_out'].replace('-', '0'))
            d['bytes_in'] = int(d['bytes_in'].replace('-', '0'))
            d['code'] = int(d['code'])
        except ValueError:
            return {}
        d['log_source'] = log_source
        return d

    def delete_output_directory(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def save_processed_files_list(self):
        pickle_file = os.path.join(self.upload_dir, 'processed_files.pickle')
        if not os.path.exists(pickle_file):
            return

        new_processed_files = cPickle.load(open(pickle_file, 'r'))

        if len(new_processed_files) > 0:
            processed_files = self.load_already_processed_files()
            if processed_files is None:
                raise Exception("Unable to get processed files list")

            processed_files.update(new_processed_files)
            if not self.save_processed_files(processed_files):
                raise Exception("Unable to save processed files list")

        os.remove(pickle_file)

    def get_info_files(self):
        if os.path.exists(self.upload_dir):
            for root, dirs, files in os.walk(self.upload_dir):
                for f in files:
                    if f.endswith('.info'):
                        yield os.path.join(root, f)

    def deliver_log(self, info_file):
        info = InfoFile(info_file)

        if not os.path.exists(info.upload_file):
            raise Exception('Found info file without upload file: %s' %
                            (info_file))

        hash_ = calculate_filehash(open(info.upload_file, 'r'))

        target_name = '%s/%s/%s.log.gz' % \
            (info.container_name.encode('utf-8'), info.timestamp, hash_)

        target_account = info.account
        if self.test_mode:
            target_account = self.test_target_account
        if len(target_name) > MAX_OBJECT_NAME_LENGTH:
            self.logger.debug('Bad target_name: %d chars' % len(target_name))
        elif not self.internal_proxy.upload_file(
                info.upload_file, target_account, self.target_container,
                target_name):
            raise Exception('Uploading to swift failed: %s/%s/%s : %s' % (
                target_account, self.target_container,
                target_name, info.account))

        info.delete()

    def deliver_logs(self):
        if not os.path.exists(self.upload_dir):
            return

        pickle_file = os.path.join(self.upload_dir, 'processed_files.pickle')
        if os.path.exists(pickle_file):
            raise Exception('Pickle file exists: %s' % (pickle_file))

        # deliver logs
        pool = multiprocessing.Pool(self.worker_count)
        pool_data = [(self.conf, AccessLogDelivery, (self.conf,),
                     'deliver_log', (x,)) for x in self.get_info_files()]

        success = True
        for result in pool.imap(multiprocess_process, pool_data):
            success = success and result
        pool.close()
        pool.join()

        if not success:
            raise Exception('Unable to deliver logs')

        shutil.rmtree(self.upload_dir)

    def process_logs(self):
        # check for output and upload directories
        if os.path.exists(self.output_dir):
            raise Exception('Output directory %s exists' % self.output_dir)

        if os.path.exists(self.upload_dir):
            raise Exception('Upload directory %s exists' % self.upload_dir)

        # get logs to process
        processed_files = self.load_already_processed_files()
        if processed_files is None:
            raise Exception('Unable to get processed files list')

        logs_to_process = \
            self.get_logs_to_process(processed_files)
        if not logs_to_process:
            return

        # make output directory
        os.makedirs(self.output_dir)

        # write log lines to deliver
        pool = multiprocessing.Pool(self.worker_count)
        pool_data = [(self.conf, AccessLogDelivery, (self.conf,),
                     'process_one_object', x) for x in logs_to_process]

        success = True
        for result in pool.imap(multiprocess_process, pool_data):
            success = success and result
        pool.close()
        pool.join()

        if not success:
            raise Exception('Unable to process logs')

        # write processed pickle file
        processed_files = set()
        for item in logs_to_process:
            a, c, o = item
            processed_files.add(o)
        pickle_file = os.path.join(self.output_dir, 'processed_files.pickle')
        cPickle.dump(processed_files, open(pickle_file, 'w'),
                     cPickle.HIGHEST_PROTOCOL)

        # copy output directory to upload directory
        shutil.move(self.output_dir, self.upload_dir)


class AccessLogDeliveryDaemon(Daemon):
    """
    Processes access (proxy) logs to split them up by account and deliver the
    split logs to their respective accounts.
    """

    def __init__(self, c):
        self.conf = c
        super(AccessLogDeliveryDaemon, self).__init__(c)
        self.logger = get_logger(c, log_route='access-log-delivery')
        self.log_processor = AccessLogDelivery(c)
        self.run_frequency = int(c.get('run_frequency', '3600'))
        self.lock_file = c.get(
            'lock_file', '/var/run/swift/swift-access-log-delivery.lock')
        self.lock_sleep = int(c.get('lock_sleep', 10))

    def process_filesystem_artifacts(self):
        with Stopwatch(self.logger, 'Deleting output directory'):
            try:
                self.log_processor.delete_output_directory()
            except Exception:
                self.logger.exception("Unable to delete output directory")
                return False

        with Stopwatch(self.logger, 'Saving processed files list'):
            try:
                self.log_processor.save_processed_files_list()
            except Exception:
                self.logger.exception("Unable to save processed files "
                                      "list")
                return False

        with Stopwatch(self.logger, 'Delivering logs'):
            try:
                self.log_processor.deliver_logs()
            except Exception:
                self.logger.exception("Unable to deliver logs")
                return False
        return True

    def process_logs(self):
        with Stopwatch(self.logger, 'Processing logs'):
            try:
                self.log_processor.process_logs()
            except Exception:
                self.logger.exception("Unable to process logs")

    def run_once(self, *a, **kw):
        global global_logger
        global_logger = self.logger

        if self.process_filesystem_artifacts():
            self.process_logs()
            self.process_filesystem_artifacts()

    def run_forever(self, *a, **kw):
        while True:
            start_time = time.time()
            try:
                with lock_file(self.lock_file, 0, unlink=False):
                    self.run_once()
            except LockTimeout:
                self.logger.info('Unable to get run lock')
                time.sleep(self.lock_sleep)
                continue
            end_time = time.time()
            # don't run more than once every self.run_frequency seconds
            sleep_time = self.run_frequency - (end_time - start_time)
            time.sleep(max(0, sleep_time))
