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

# TODO: Tests

import unittest
from contextlib import contextmanager

from test.unit import temptree
from slogging import access_log_delivery


class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def keys(self):
        return self.store.keys()

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


class TestAccessLogDelivery(unittest.TestCase):

    def test_log_line_parser_query_args(self):
        p = access_log_delivery.AccessLogDelivery({}, DumbLogger())
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {'code': 8, 'processing_time': '17', 'auth_token': '11',
                    'month': '01', 'second': '6', 'year': '3', 'tz': '+0000',
                    'http_version': '7', 'object_name': 'o', 'etag': '14',
                    'method': '5', 'trans_id': '15', 'client_ip': '2',
                    'bytes_out': 13, 'container_name': 'c', 'day': '1',
                    'minute': '5', 'account': 'a', 'hour': '4',
                    'referrer': '9', 'request': '/v1/a/c/o?foo',
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3'}
        self.assertEquals(res, expected)

    def test_log_line_parser_hidden_ip(self):
        conf = {'hidden_ips': '1.2.3.4'}
        p = access_log_delivery.AccessLogDelivery(conf, DumbLogger())
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[2] = '1.2.3.4'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = '0.0.0.0'
        self.assertEquals(res['client_ip'], expected)
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[2] = '4.3.2.1'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = '4.3.2.1'
        self.assertEquals(res['client_ip'], expected)

    def test_log_line_parser_field_count(self):
        p = access_log_delivery.AccessLogDelivery({}, DumbLogger())
        # too few fields
        log_line = [str(x) for x in range(17)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {}
        self.assertEquals(res, expected)
        # right amount of fields
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {'code': 8, 'processing_time': '17', 'auth_token': '11',
                    'month': '01', 'second': '6', 'year': '3', 'tz': '+0000',
                    'http_version': '7', 'object_name': 'o', 'etag': '14',
                    'method': '5', 'trans_id': '15', 'client_ip': '2',
                    'bytes_out': 13, 'container_name': 'c', 'day': '1',
                    'minute': '5', 'account': 'a', 'hour': '4',
                    'referrer': '9', 'request': '/v1/a/c/o',
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3'}
        self.assertEquals(res, expected)
        # too many fields
        log_line = [str(x) for x in range(19)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {'code': 8, 'processing_time': '17', 'auth_token': '11',
                    'month': '01', 'second': '6', 'year': '3', 'tz': '+0000',
                    'http_version': '7', 'object_name': 'o', 'etag': '14',
                    'method': '5', 'trans_id': '15', 'client_ip': '2',
                    'bytes_out': 13, 'container_name': 'c', 'day': '1',
                    'minute': '5', 'account': 'a', 'hour': '4',
                    'referrer': '9', 'request': '/v1/a/c/o',
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3'}
        self.assertEquals(res, expected)

    def test_make_clf_from_parts(self):
        p = access_log_delivery.AccessLogDelivery({}, DumbLogger())
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        parts = p.log_line_parser(log_line)
        clf = access_log_delivery.make_clf_from_parts(parts)
        expect = '2 - - [1/01/3:4:5:6 +0000] "5 /v1/a/c/o?foo 7" 8 13 "9" "10"'
        self.assertEquals(clf, expect)

    def test_convert_log_line(self):
        p = access_log_delivery.AccessLogDelivery({}, DumbLogger())
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        res = p.convert_log_line(log_line)
        expected = (
            '2 - - [1/01/3:4:5:6 +0000] "5 /v1/a/c/o?foo 7" 8 13 "9" "10"',
            'a',
            'c')
        self.assertEquals(res, expected)

    def test_get_container_save_log_flag(self):
        p = access_log_delivery.AccessLogDelivery({}, DumbLogger())

        def my_get_metadata_true(*a, **kw):
            return {p.metadata_key: 'yes'}

        def my_get_metadata_true_upper(*a, **kw):
            return {p.metadata_key: 'YES'}

        def my_get_metadata_false(*a, **kw):
            return {p.metadata_key: 'no'}
        p.internal_proxy.get_container_metadata = my_get_metadata_false
        p.memcache = FakeMemcache()
        res = p.get_container_save_log_flag('a', 'c1')
        expected = False
        self.assertEquals(res, expected)
        p.internal_proxy.get_container_metadata = my_get_metadata_true
        p.memcache = FakeMemcache()
        res = p.get_container_save_log_flag('a', 'c2')
        expected = True
        self.assertEquals(res, expected)
        p.internal_proxy.get_container_metadata = my_get_metadata_true_upper
        p.memcache = FakeMemcache()
        res = p.get_container_save_log_flag('a', 'c3')
        expected = True
        self.assertEquals(res, expected)

    def test_process_one_file(self):
        with temptree([]) as t:
            conf = {'working_dir': t}
            p = access_log_delivery.AccessLogDelivery(conf, DumbLogger())

            def my_get_object_data(*a, **kw):
                all_lines = []
                log_line = [str(x) for x in range(18)]
                log_line[1] = 'proxy-server'
                log_line[4] = '1/Jan/3/4/5/6'
                log_line[6] = '/v1/a/c/o?foo'
                yield 'x' * 16 + ' '.join(log_line)

                log_line = [str(x) for x in range(18)]
                log_line[1] = 'proxy-server'
                log_line[4] = '1/Jan/3/4/5/6'
                log_line[6] = '/v1/a/c/o'
                yield 'x' * 16 + ' '.join(log_line)

                log_line = [str(x) for x in range(18)]
                log_line[1] = 'proxy-server'
                log_line[4] = '1/Jan/3/4/5/6'
                log_line[6] = '/v1/a2/c2/o2'
                yield 'x' * 16 + ' '.join(log_line)

            def my_get_container_save_log_flag(*a, **kw):
                return True
            p.get_object_data = my_get_object_data
            p.get_container_save_log_flag = my_get_container_save_log_flag
            res = p.process_one_file('a', 'c', '2011/03/14/12/hash')
            expected = ['%s/a2/c2/2011/03/14/12' % t,
                        '%s/a/c/2011/03/14/12' % t]
            self.assertEquals(res, set(expected))
            lines = [p.convert_log_line(x)[0] for x in my_get_object_data()]
            with open(expected[0], 'rb') as f:
                raw = f.read()
                res = '\n'.join(lines[2:]) + '\n'
                self.assertEquals(res, raw)
            with open(expected[1], 'rb') as f:
                raw = f.read()
                res = '\n'.join(lines[:2]) + '\n'
                self.assertEquals(res, raw)


if __name__ == '__main__':
    unittest.main()
