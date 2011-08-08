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

import unittest
from slogging import stats_processor


class TestStatsProcessor(unittest.TestCase):

    def test_process(self):
        p = stats_processor.StatsLogProcessor({})
        test_obj_stream = ['a, 1, 1, 1', 'a, 1, 1, 1']
        res = p.process(test_obj_stream, 'foo', 'bar', '2011/03/14/12/baz')
        expected = {('a', '2011', '03', '14', '12'):
                        {'object_count': 2, 'container_count': 2,
                        'replica_count': 2, 'bytes_used': 2}}
        self.assertEquals(res, expected)

    def test_process_extra_columns(self):
        p = stats_processor.StatsLogProcessor({})
        test_obj_stream = ['a, 1, 1, 1, extra']
        res = p.process(test_obj_stream, 'foo', 'bar', '2011/03/14/12/baz')
        expected = {('a', '2011', '03', '14', '12'):
                        {'object_count': 1, 'container_count': 1,
                        'replica_count': 1, 'bytes_used': 1}}
        self.assertEquals(res, expected)

    def test_process_bad_line(self):
        p = stats_processor.StatsLogProcessor({})
        test_obj_stream = ['a, 1, 1, 1, extra', '', 'some bad line']
        res = p.process(test_obj_stream, 'foo', 'bar', '2011/03/14/12/baz')
        expected = {('a', '2011', '03', '14', '12'):
                        {'object_count': 1, 'container_count': 1,
                        'replica_count': 1, 'bytes_used': 1}}
        self.assertEquals(res, expected)


if __name__ == '__main__':
    unittest.main()
