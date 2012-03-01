import unittest
import Queue
from slogging import log_common

from slogging import log_processor


class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass


class DumbInternalProxy(object):
    def __init__(self, code=200, timeout=False, bad_compressed=False):
        self.code = code
        self.timeout = timeout
        self.bad_compressed = bad_compressed

    def get_container_list(self, account, container, marker=None,
                           end_marker=None):
        n = '2010/03/14/13/obj1'
        if marker is None or n > marker:
            if end_marker:
                if n <= end_marker:
                    return [{'name': n}]
                else:
                    return []
            return [{'name': n}]
        return []

    def get_object(self, account, container, object_name):
        if object_name.endswith('.gz'):
            if self.bad_compressed:
                # invalid compressed data
                def data():
                    yield '\xff\xff\xff\xff\xff\xff\xff'
            else:
                # 'obj\ndata', compressed with gzip -9
                def data():
                    yield '\x1f\x8b\x08'
                    yield '\x08"\xd79L'
                    yield '\x02\x03te'
                    yield 'st\x00\xcbO'
                    yield '\xca\xe2JI,I'
                    yield '\xe4\x02\x00O\xff'
                    yield '\xa3Y\t\x00\x00\x00'
        else:
            def data():
                yield 'obj\n'
                if self.timeout:
                    raise ChunkReadTimeout
                yield 'data'
        return self.code, data()


class TestLogProcessor(unittest.TestCase):

    proxy_config = {'log-processor': {

                    }
                   }
    access_test_line = 'Jul  9 04:14:30 saio proxy-server 1.2.3.4 4.5.6.7 '\
                    '09/Jul/2010/04/14/30 GET '\
                    '/v1/acct/foo/bar?format=json&foo HTTP/1.0 200 - '\
                    'curl tk4e350daf-9338-4cc6-aabb-090e49babfbd '\
                    '6 95 - txfa431231-7f07-42fd-8fc7-7da9d8cc1f90 - 0.0262'

    def test_collate_worker(self):
        try:
            log_processor.LogProcessor._internal_proxy = DumbInternalProxy()

            def get_object_data(*a, **kw):
                return [self.access_test_line]
            orig_get_object_data = log_processor.LogProcessor.get_object_data
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format': '%Y%m%d%H*',
                        'class_path':
                            'slogging.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            q_in = Queue.Queue()
            q_in.close = lambda: None
            q_out = Queue.Queue()
            q_out.close = lambda: None
            work_request = ('access', 'a', 'c', 'o')
            q_in.put(work_request)
            q_in.put(None)
            processor_klass = log_processor.LogProcessor
            log_common.collate_worker(processor_klass, processor_args,
                                      'process_one_file', q_in, q_out,
                                      DumbLogger())
            item, ret = q_out.get()
            self.assertEquals(item, work_request)
            expected = {('acct', '2010', '07', '09', '04'):
                        {('public', 'object', 'GET', '2xx'): 1,
                        ('public', 'bytes_out'): 95,
                        'marker_query': 0,
                        'format_query': 1,
                        'delimiter_query': 0,
                        'path_query': 0,
                        ('public', 'bytes_in'): 6,
                        'prefix_query': 0}}
            self.assertEquals(ret, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_collate_worker_error(self):
        def get_object_data(*a, **kw):
            raise Exception()
        orig_get_object_data = log_processor.LogProcessor.get_object_data
        try:
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format': '%Y%m%d%H*',
                        'class_path':
                            'slogging.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            q_in = Queue.Queue()
            q_in.close = lambda: None
            q_out = Queue.Queue()
            q_out.close = lambda: None
            work_request = ('access', 'a', 'c', 'o')
            q_in.put(work_request)
            q_in.put(None)
            processor_klass = log_processor.LogProcessor
            log_common.collate_worker(processor_klass, processor_args,
                                      'process_one_file', q_in, q_out,
                                      DumbLogger())
            item, ret = q_out.get()
            self.assertEquals(item, work_request)
            # these only work for Py2.7+
            #self.assertIsInstance(ret, log_common.BadFileDownload)
            self.assertTrue(isinstance(ret, Exception))
        finally:
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_multiprocess_collate(self):
        try:
            log_processor.LogProcessor._internal_proxy = DumbInternalProxy()

            def get_object_data(*a, **kw):
                return [self.access_test_line]
            orig_get_object_data = log_processor.LogProcessor.get_object_data
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format': '%Y%m%d%H*',
                        'class_path':
                            'slogging.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            item = ('access', 'a', 'c', 'o')
            logs_to_process = [item]
            processor_klass = log_processor.LogProcessor
            results = log_processor.multiprocess_collate(processor_klass,
                                                         processor_args,
                                                         'process_one_file',
                                                         logs_to_process,
                                                         1,
                                                         DumbLogger())
            results = list(results)
            expected = [(item, {('acct', '2010', '07', '09', '04'):
                        {('public', 'object', 'GET', '2xx'): 1,
                        ('public', 'bytes_out'): 95,
                        'marker_query': 0,
                        'format_query': 1,
                        'delimiter_query': 0,
                        'path_query': 0,
                        ('public', 'bytes_in'): 6,
                        'prefix_query': 0}})]
            self.assertEquals(results, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_multiprocess_collate_errors(self):
        def get_object_data(*a, **kw):
            raise log_common.BadFileDownload()
        orig_get_object_data = log_processor.LogProcessor.get_object_data
        try:
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format': '%Y%m%d%H*',
                        'class_path':
                            'slogging.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            item = ('access', 'a', 'c', 'o')
            logs_to_process = [item]
            processor_klass = log_processor.LogProcessor
            results = log_common.multiprocess_collate(processor_klass,
                                                      processor_args,
                                                      'process_one_file',
                                                      logs_to_process,
                                                      1,
                                                      DumbLogger())
            results = list(results)
            expected = []
            self.assertEquals(results, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data
