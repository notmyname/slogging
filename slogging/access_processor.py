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

import collections
from urllib import unquote
import copy
from tzlocal import get_localzone
from datetime import datetime
from slogging import common
import pytz
from urlparse import urlparse

# conditionalize the return_ips method based on whether or not iptools
# is present in the system. Without iptools, you will lack CIDR support.
try:
    from iptools import IpRangeList
    CIDR_support = True

    def return_ips(conf, conf_tag):
        return set(k for k in
                    IpRangeList(*[x.strip() for x in
                    conf.get(conf_tag, '').split(',') if x.strip()]))
    def sanitize_ips(line_data):
        for x in ['lb_ip', 'client_ip', 'log_source']:
            if line_data[x] == '-':
                line_data[x] = '0.0.0.0'
except ImportError:
    CIDR_support = False

    def return_ips(conf, conf_tag):
        return ([x.strip() for x in conf.get(conf_tag, '').split(',')
            if x.strip()])

from swift.common.utils import split_path, get_logger

month_map = '_ Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
LISTING_PARAMS = set(
                'path limit format delimiter marker end_marker prefix'.split())
local_zone = get_localzone()


class AccessLogProcessor(object):
    """Transform proxy server access logs"""

    def __init__(self, conf):
        self.server_name = conf.get('server_name', 'proxy-server')
        for conf_tag in ['lb_private_ips', 'service_ips']:
            setattr(self, conf_tag, return_ips(conf, conf_tag))
        self.warn_percent = float(conf.get('warn_percent', '0.8'))
        self.logger = get_logger(conf, log_route='access-processor')
        self.time_zone = common.get_time_zone(conf, self.logger, 'time_zone',
                                              str(local_zone))

    def log_line_parser(self, raw_log):
        '''given a raw access log line, return a dict of the good parts'''
        d = {}
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
            self.logger.debug(_('Bad line data: %s') % repr(raw_log))
            return {}
        if server != self.server_name:
            # incorrect server name in log line
            self.logger.debug(_('Bad server name: found "%(found)s" ' \
                    'expected "%(expected)s"') %
                    {'found': server, 'expected': self.server_name})
            return {}
        try:
            parsed_url = urlparse(request)
            request = parsed_url.path
            query = parsed_url.query
            (version, account, container_name, object_name) = \
                split_path(request, 2, 4, True)
        except ValueError, e:
            self.logger.debug(_('Invalid path: %(error)s from data: %(log)s') %
            {'error': e, 'log': repr(raw_log)})
            return {}
        if version != 'v1':
            # "In the wild" places this can be caught are with auth systems
            # that use the same endpoint as the rest of the Swift API (eg
            # tempauth or swauth). But if the Swift API ever does change, this
            # protects that too.
            self.logger.debug(_('Unexpected Swift version string: found ' \
                                '"%s" expected "v1"') % version)
            return {}
        if query != "":
            args = query.split('&')
            d['query'] = query
            # Count each query argument. This is used later to aggregate
            # the number of format, prefix, etc. queries.
            for q in args:
                if '=' in q:
                    k, v = q.split('=', 1)
                else:
                    k = q
                # Certain keys will get summmed in stats reporting
                # (format, path, delimiter, etc.). Save a "1" here
                # to indicate that this request is 1 request for
                # its respective key.
                if k in LISTING_PARAMS:
                    d[k] = 1
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
        d['log_source'] = log_source
        return d

    def process(self, obj_stream, data_object_account, data_object_container,
                data_object_name):
        '''generate hourly groupings of data from one access log file'''
        hourly_aggr_info = {}
        total_lines = 0
        bad_lines = 0
        for line in obj_stream:
            line_data = self.log_line_parser(line)
            total_lines += 1
            if not line_data:
                bad_lines += 1
                continue
            account = line_data['account']
            container_name = line_data['container_name']
            year = line_data['year']
            month = line_data['month']
            day = line_data['day']
            hour = line_data['hour']
            bytes_out = line_data['bytes_out']
            bytes_in = line_data['bytes_in']
            method = line_data['method']
            code = int(line_data['code'])
            object_name = line_data['object_name']
            client_ip = line_data['client_ip']

            op_level = None
            if not container_name:
                op_level = 'account'
            elif container_name and not object_name:
                op_level = 'container'
            elif object_name:
                op_level = 'object'

            utc_line_date = datetime(int(year), int(month), int(day),
                                     int(hour), 0, 0, tzinfo=pytz.utc)
            line_date = utc_line_date.astimezone(self.time_zone)
            line_date_year = line_date.strftime('%Y')
            line_date_month = line_date.strftime('%m')
            line_date_day = line_date.strftime('%d')
            line_date_hour = line_date.strftime('%H')

            aggr_key = (account, line_date_year, line_date_month,
                        line_date_day, line_date_hour)
            d = hourly_aggr_info.get(aggr_key, {})
            if CIDR_support:
                sanitize_ips(line_data)
            if line_data['lb_ip'] in self.lb_private_ips or \
                    line_data['client_ip'] in self.service_ips or \
                    line_data['log_source'] not in ['-', None]:
                source = 'service'
            else:
                source = 'public'

            d[(source, 'bytes_out')] = d.setdefault((
                source, 'bytes_out'), 0) + bytes_out
            d[(source, 'bytes_in')] = d.setdefault((source, 'bytes_in'), 0) + \
                                      bytes_in

            d['format_query'] = d.setdefault('format_query', 0) + \
                                line_data.get('format', 0)
            d['marker_query'] = d.setdefault('marker_query', 0) + \
                                line_data.get('marker', 0)
            d['prefix_query'] = d.setdefault('prefix_query', 0) + \
                                line_data.get('prefix', 0)
            d['delimiter_query'] = d.setdefault('delimiter_query', 0) + \
                                   line_data.get('delimiter', 0)
            path = line_data.get('path', 0)
            d['path_query'] = d.setdefault('path_query', 0) + path

            code = '%dxx' % (code / 100)
            key = (source, op_level, method, code)
            d[key] = d.setdefault(key, 0) + 1

            hourly_aggr_info[aggr_key] = d
        if bad_lines > (total_lines * self.warn_percent):
            name = '/'.join([data_object_account, data_object_container,
                             data_object_name])
            self.logger.warning(_('I found a bunch of bad lines in %(name)s '\
                    '(%(bad)d bad, %(total)d total)') %
                    {'name': name, 'bad': bad_lines, 'total': total_lines})
        return hourly_aggr_info

    def keylist_mapping(self):
        source_keys = 'service public'.split()
        level_keys = 'account container object'.split()
        verb_keys = 'GET PUT POST DELETE HEAD COPY'.split()
        code_keys = '2xx 4xx 5xx'.split()

        keylist_mapping = {
        #   <db key> : <row key> or <set of row keys>
            'service_bw_in': ('service', 'bytes_in'),
            'service_bw_out': ('service', 'bytes_out'),
            'public_bw_in': ('public', 'bytes_in'),
            'public_bw_out': ('public', 'bytes_out'),
            'account_requests': set(),
            'container_requests': set(),
            'object_requests': set(),
            'service_request': set(),
            'public_request': set(),
            'ops_count': set(),
        }
        for verb in verb_keys:
            keylist_mapping[verb] = set()
        for code in code_keys:
            keylist_mapping[code] = set()
        for source in source_keys:
            for level in level_keys:
                for verb in verb_keys:
                    for code in code_keys:
                        keylist_mapping['account_requests'].add(
                                        (source, 'account', verb, code))
                        keylist_mapping['container_requests'].add(
                                        (source, 'container', verb, code))
                        keylist_mapping['object_requests'].add(
                                        (source, 'object', verb, code))
                        keylist_mapping['service_request'].add(
                                        ('service', level, verb, code))
                        keylist_mapping['public_request'].add(
                                        ('public', level, verb, code))
                        keylist_mapping[verb].add(
                                        (source, level, verb, code))
                        keylist_mapping[code].add(
                                        (source, level, verb, code))
                        keylist_mapping['ops_count'].add(
                                        (source, level, verb, code))
        return keylist_mapping
