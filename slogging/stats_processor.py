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


class StatsLogProcessor(object):
    """Transform account storage stat logs"""

    def __init__(self, conf, logger=None):
        self.logger = logger

    def process(self, obj_stream, data_object_account, data_object_container,
                data_object_name):
        '''generate hourly groupings of data from one stats log file'''
        account_totals = {}
        year, month, day, hour, _junk = data_object_name.split('/')
        start_time = time.time()
        line_count = 0
        for line in obj_stream:
            if not line:
                continue
            line_count += 1
            try:
                (account,
                container_count,
                object_count,
                bytes_used) = line.split(',')[:4]
                account = account.strip('"')
                container_count = int(container_count.strip('"'))
                object_count = int(object_count.strip('"'))
                bytes_used = int(bytes_used.strip('"'))
            except (IndexError, ValueError):
                # bad line data
                if self.logger:
                    self.logger.debug(_('Bad line data: %s') % repr(line))
                continue
            aggr_key = (account, year, month, day, hour)
            d = account_totals.get(aggr_key, {})
            d['replica_count'] = d.setdefault('replica_count', 0) + 1
            d['container_count'] = d.setdefault('container_count', 0) + \
                                   container_count
            d['object_count'] = d.setdefault('object_count', 0) + \
                                object_count
            d['bytes_used'] = d.setdefault('bytes_used', 0) + \
                              bytes_used
            account_totals[aggr_key] = d
        processing_time = time.time() - start_time
        if self.logger:
            self.logger.info('Processed %d lines in %.1f seconds '
                '(%.1f per sec)' % (line_count, processing_time,
                line_count / processing_time))
        return account_totals

    def keylist_mapping(self):
        '''
        returns a dictionary of final keys mapped to source keys
        '''
        keylist_mapping = {
        #   <db key> : <row key> or <set of row keys>
            'bytes_used': 'bytes_used',
            'container_count': 'container_count',
            'object_count': 'object_count',
            'replica_count': 'replica_count',
        }
        return keylist_mapping
