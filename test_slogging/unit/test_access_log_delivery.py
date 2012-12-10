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
from contextlib import contextmanager
import json
import StringIO

from test.unit import temptree
from swift.common import utils
from slogging import access_log_delivery as ald
import utils as test_utils


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

class TestInfoFile(test_utils.MockerTestCase):
    def test_init(self):
        fields = {
            'account': 'some_account',
            'container_name': 'some_container',
            'year': 2012,
            'month': '05',
            'day': '04',
            'hour': '03',
        }

        info = ald.InfoFile('some_file.info', fields)
        self.assertEquals(info.filename, 'some_file.info')
        for field in 'account container_name'.split():
            self.assertEquals(fields[field], getattr(info, field))
        for field in 'year month day hour'.split():
            self.assertEquals(int(fields[field]), getattr(info, field))

    def test_init_bad_filename(self):
        self.assertRaises(ValueError, ald.InfoFile, 'some_file', {})

    def test_init_missing_field(self):
        self.assertRaises(KeyError, ald.InfoFile, 'some_file.info', {})
        fields = {'account': '', 'container_name': ''}
        self.assertRaises(KeyError, ald.InfoFile, 'some_file.info', fields)

    def test_init_read(self):
        class InfoFile(ald.InfoFile):
            def read(self):
                self.read_called = True

        info = InfoFile('some_file.info')
        self.assert_(getattr(info, 'read_called', False))

    def test_delete(self):
        class InfoFile(ald.InfoFile):
            def __init__(self, filename):
                self.filename = filename

            def read(self):
                pass

        class Os(object):
            def __init__(self, test, filename):
                self.test = test
                self.filename = filename

            def remove(self, filename):
                self.remove_called = True
                self.test.assertEquals(self.filename, filename)

        filename = 'some_file.info'

        os = Os(self, filename)
        self.mock(ald, 'os', os)

        info = InfoFile(filename)
        info.delete()
        self.assert_(getattr(os, 'remove_called', False))

    def test_read(self):
        class InfoFile(ald.InfoFile):
            def __init__(self, filename):
                self.filename = filename
                self.fields = 'account container_name year month day ' \
                    'hour'.split()

        fields = {
            'account': u'some_account',
            'container_name': u'some_container',
            'year': 2012,
            'month': 10,
            'day': 9,
            'hour': 8,
        }

        class Methods(object):
            def __init__(self, test, filename):
                self.test = test
                self.filename = filename
                self.path = self

            def exists(self, filename):
                self.exists_called = True
                self.test.assertEquals(self.filename, filename)
                return True

        class Open(object):
            def __init__(self, test, filename, mode, fd):
                self.test = test
                self.filename = filename
                self.mode = mode
                self.fd = fd

            def __enter__(self):
                return self.fd

            def __call__(self, filename, mode):
                self.open_called = True
                self.test.assertEquals(self.filename, filename)
                self.test.assertEquals(self.mode, mode)
                return self

            def __exit__(self, type, value, traceback):
                pass

        filename = 'some_file.info'

        methods = Methods(self, filename)
        open_ = Open(self, filename, 'r', StringIO.StringIO(json.dumps(fields)))

        self.mock(ald, 'os', methods)
        self.mock(ald, 'open', open_)

        info = InfoFile(filename)
        info.read()

        for k, v in fields.iteritems():
            self.assertEquals(v, getattr(info, k))

        self.assert_(getattr(methods, 'exists_called', False))
        self.assert_(getattr(open_, 'open_called', False))

    def test_read_file_does_not_exist(self):
        class InfoFile(ald.InfoFile):
            def __init__(self, filename):
                self.filename = filename

        class Os(object):
            def __init__(self, test, filename):
                self.test = test
                self.filename = filename
                self.path = self

            def exists(self, filename):
                self.exists_called = True
                self.test.assertEquals(self.filename, filename)
                return False

        filename = 'some_file.info'

        os = Os(self, filename)
        self.mock(ald, 'os', os)

        info = InfoFile(filename)
        self.assertRaises(RuntimeError, info.read)
        self.assert_(getattr(os, 'exists_called', False))

    def test_timestamp(self):
        class InfoFile(ald.InfoFile):
            def __init__(self):
                self.year = 2012
                self.month = 9
                self.day = 8
                self.hour = 7

        info = InfoFile()
        self.assertEquals('2012/09/08/07', info.timestamp)

    def test_upload_file(self):
        class InfoFile(ald.InfoFile):
            def __init__(self, filename):
                self.filename = filename

        info = InfoFile('something.info')
        self.assertEquals('something', info.upload_file)

    def test_write(self):
        fields = {
            'account': u'some_account',
            'container_name': u'some_container',
            'year': 2012,
            'month': 10,
            'day': 9,
            'hour': 8,
        }

        class Methods(object):
            def __init__(self, test, filename, fields):
                self.test = test
                self.filename = filename
                self.fields = fields
                self.dirname_return = 'some_dir'
                self.fd = 'some_fd'
                self.path = self

            def __call__(self, filename, unlink=True):
                self.call_called = True
                self.test.assertEquals(self.filename, filename)
                self.test.assertEquals(False, unlink)
                return self

            def __enter__(self):
                self.enter_called = True
                return self.fd

            def __exit__(self, type, value, traceback):
                self.exit_called = True

            def dump(self, data, fd):
                self.dump_called = True
                self.test.assertEquals(self.fields, data)
                self.test.assertEquals(self.fd, fd)

            def exists(self, dirname):
                self.exists_called = True
                self.test.assertEquals(self.dirname_return, dirname)
                return False

            def dirname(self, filename):
                self.dirname_called = True
                self.test.assertEquals(self.filename, filename)
                return self.dirname_return

            def makedirs(self, dirname):
                self.makedirs_called = True
                self.test.assertEquals(self.dirname_return, dirname)

        filename = 'some_file.info'
        methods = Methods(self, filename, fields)

        self.mock(ald, 'os', methods)
        self.mock(ald, 'json', methods)
        self.mock(ald, 'lock_file', methods)

        info = ald.InfoFile(filename, fields)
        info.write()

        self.assert_(getattr(methods, 'dump_called', False))
        self.assert_(getattr(methods, 'exists_called', False))
        self.assert_(getattr(methods, 'dirname_called', False))
        self.assert_(getattr(methods, 'makedirs_called', False))
        self.assert_(getattr(methods, 'call_called', False))
        self.assert_(getattr(methods, 'enter_called', False))
        self.assert_(getattr(methods, 'exit_called', False))


class TestAccessLogDelivery(unittest.TestCase):

    conf = {'swift_account': 'foo',
            'log_source_account': 'bar'}

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'

    def test_get_logs_to_process(self):
        class AccessLogDelivery(ald.AccessLogDelivery):
            def __init__(self, test):
                self.test = test
                self.source_account = 'some_account'
                self.source_container = 'some_container'
                self.lookback_start = 'some_start'
                self.lookback_end = 'some_end'
                self.logs_to_process_per_run = 0
                self.container_listing = 'a b c d'.split()
                self.already_processed_files = 'e f g'.split()
                self.logger = DumbLogger()

            def calculate_lookback(self):
                return self.lookback_start, self.lookback_end

            def get_container_listing(self, account, container,
                lookback_start, lookback_end, already_processed_files):
                self.test.assertEquals(self.source_account, account)
                self.test.assertEquals(self.source_container, container)
                self.test.assertEquals(self.lookback_start, lookback_start)
                self.test.assertEquals(self.lookback_end, lookback_end)
                self.test.assertEquals(self.already_processed_files,
                    already_processed_files)
                return self.container_listing

        acld = AccessLogDelivery(self)
        logs = acld.get_logs_to_process(acld.already_processed_files)
        expected_logs = [(acld.source_account, acld.source_container, x) for
            x in acld.container_listing]
        self.assertEquals(expected_logs, logs)

    def test_get_logs_to_process_with_limit(self):
        class AccessLogDelivery(ald.AccessLogDelivery):
            def __init__(self):
                self.source_account = 'some_account'
                self.source_container = 'some_container'
                self.container_listing = 'a b c d'.split()
                self.logs_to_process_per_run = 2
                self.logger = DumbLogger()

            def calculate_lookback(self):
                return None, None

            def get_container_listing(self, *args):
                return self.container_listing

        acld = AccessLogDelivery()
        logs = acld.get_logs_to_process([])
        expected_logs = [(acld.source_account, acld.source_container, x) for
            x in acld.container_listing[:acld.logs_to_process_per_run]]
        self.assertEquals(expected_logs, logs)

    def test_log_line_parser_query_args(self):
        p = ald.AccessLogDelivery(self.conf, DumbLogger())
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
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3',
                    'log_source': None,}
        self.assertEquals(res, expected)

    def test_log_line_parser_hidden_ip(self):
        conf = {'hidden_ips': '1.2.3.4', 'swift_account': 'foo',
                'log_source_account': 'bar'}
        p = ald.AccessLogDelivery(conf, DumbLogger())
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
        p = ald.AccessLogDelivery(self.conf, DumbLogger())
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
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3',
                    'log_source': None,}
        self.assertEquals(res, expected)
        # with log_source
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
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3',
                    'log_source': '18',}
        self.assertEquals(res, expected)

    def test_make_clf_from_parts(self):
        p = ald.AccessLogDelivery(self.conf, DumbLogger())
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o?foo'
        log_line = 'x' * 16 + ' '.join(log_line)
        parts = p.log_line_parser(log_line)
        clf = ald.make_clf_from_parts(parts)
        expect = '2 - - [1/01/3:4:5:6 +0000] "5 /v1/a/c/o?foo 7" 8 13 "9" "10"'
        self.assertEquals(clf, expect)

    def test_make_clf_from_parts_keyerror(self):
        self.assertEquals(None, ald.make_clf_from_parts({}))

#    def test_get_container_save_log_flag(self):
#        p = ald.AccessLogDelivery(self.conf, DumbLogger())
#
#        def my_get_metadata_true(*a, **kw):
#            return {p.metadata_key: 'yes'}
#
#        def my_get_metadata_true_upper(*a, **kw):
#            return {p.metadata_key: 'YES'}
#
#        def my_get_metadata_false(*a, **kw):
#            return {p.metadata_key: 'no'}
#        p.internal_proxy.get_container_metadata = my_get_metadata_false
#        p.memcache = FakeMemcache()
#        res = p.get_container_save_log_flag('a', 'c1')
#        expected = False
#        self.assertEquals(res, expected)
#        p.internal_proxy.get_container_metadata = my_get_metadata_true
#        p.memcache = FakeMemcache()
#        res = p.get_container_save_log_flag('a', 'c2')
#        expected = True
#        self.assertEquals(res, expected)
#        p.internal_proxy.get_container_metadata = my_get_metadata_true_upper
#        p.memcache = FakeMemcache()
#        res = p.get_container_save_log_flag('a', 'c3')
#        expected = True
#        self.assertEquals(res, expected)


if __name__ == '__main__':
    unittest.main()
