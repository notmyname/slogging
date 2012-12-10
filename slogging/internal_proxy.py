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

import webob
from urllib import quote, unquote
from json import loads as json_loads
import copy

from slogging.compressing_file_reader import CompressingFileReader
from swift.proxy.server import Application


def make_request_body_file(source_file, compress=True):
    if hasattr(source_file, 'seek'):
        source_file.seek(0)
    else:
        source_file = open(source_file, 'rb')
    if compress:
        compressed_file = CompressingFileReader(source_file)
        return compressed_file
    return source_file


def webob_request_copy(orig_req, source_file=None, compress=True):
    req_copy = orig_req.copy()
    req_copy.headers = dict(orig_req.headers)
    if source_file:
        req_copy.body_file = make_request_body_file(source_file,
                                                    compress=compress)
    return req_copy


class InternalProxy(object):
    """
    Set up a private instance of a proxy server that allows normal requests
    to be made without having to actually send the request to the proxy.
    This also doesn't log the requests to the normal proxy logs.

    :param proxy_server_conf: proxy server configuration dictionary
    :param logger: logger to log requests to
    :param retries: number of times to retry each request
    """

    def __init__(self, proxy_server_conf=None, logger=None, retries=0,
                 memcache=None):
        self.upload_app = Application(proxy_server_conf, memcache=memcache,
                                          logger=logger)
        self.retries = retries

    def _handle_request(self, req, source_file=None, compress=True):
        req = self.upload_app.update_request(req)
        req_copy = webob_request_copy(req, source_file=source_file,
                                      compress=compress)
        resp = self.upload_app.handle_request(req_copy)
        tries = 1
        while (resp.status_int < 200 or resp.status_int > 299) \
                and tries < self.retries:
            req_copy = webob_request_copy(req, source_file=source_file,
                                          compress=compress)
            resp = self.upload_app.handle_request(req_copy)
            tries += 1
        return resp

    def make_path(self, account, container=None, object_=None):
        if isinstance(account, unicode):
            account = account.encode('utf-8')

        if isinstance(container, unicode):
            container = container.encode('utf-8')

        if isinstance(object_, unicode):
            object_ = object_.encode('utf-8')

        path = '/v1/%s' % (quote(account))
        if container:
            path += '/%s' % (quote(container))

            if object_:
                path += '/%s' % (quote(object_))
        elif object_:
            raise ValueError('Object specified without container')
        return path

    def upload_file(self, source_file, account, container, object_name,
                    compress=True, content_type='application/x-gzip',
                    etag=None, headers=None):
        """
        Upload a file to cloud files.

        :param source_file: path to or file like object to upload
        :param account: account to upload to
        :param container: container to upload to
        :param object_name: name of object being uploaded
        :param compress: if True, compresses object as it is uploaded
        :param content_type: content-type of object
        :param etag: etag for object to check successful upload
        :returns: True if successful, False otherwise
        """
        # create the container
        if not self.create_container(account, container):
            return False

        send_headers = {'Transfer-Encoding': 'chunked'}
        if headers:
            send_headers.update(headers)

        # upload the file to the account
        req = webob.Request.blank(
                  self.make_path(account, container, object_name),
                  content_type=content_type,
                  environ={'REQUEST_METHOD': 'PUT'},
                  headers=send_headers)
        req.content_length = None   # to make sure we send chunked data
        if etag:
            req.headers['etag'] = etag
        resp = self._handle_request(req, source_file=source_file,
                                    compress=compress)
        if not (200 <= resp.status_int < 300):
            return False
        return True

    def get_object(self, account, container, object_name):
        """
        Get object.

        :param account: account name object is in
        :param container: container name object is in
        :param object_name: name of object to get
        :returns: iterator for object data
        """
        req = webob.Request.blank(
                  self.make_path(account, container, object_name),
                  environ={'REQUEST_METHOD': 'GET'})
        resp = self._handle_request(req)
        return resp.status_int, resp.app_iter

    def create_container(self, account, container):
        """
        Create container.

        :param account: account name to put the container in
        :param container: container name to create
        :returns: True if successful, otherwise False
        """
        req = webob.Request.blank(
                  self.make_path(account, container),
                  environ={'REQUEST_METHOD': 'PUT'})
        resp = self._handle_request(req)
        return 200 <= resp.status_int < 300

    def get_container_list(self, account, container, marker=None,
                           end_marker=None, limit=None, prefix=None,
                           delimiter=None, full_listing=True):
        """
        Get a listing of objects for the container.

        :param account: account name for the container
        :param container: container name to get a listing for
        :param marker: marker query
        :param end_marker: end marker query
        :param limit: limit query
        :param prefix: prefix query
        :param delimeter: string to delimit the queries on
        :param full_listing: if True, return a full listing, else returns a max
                             of 10000 listings
        :returns: list of objects
        """
        if full_listing:
            rv = []
            listing = self.get_container_list(account, container, marker,
                                              end_marker, limit, prefix,
                                              delimiter, full_listing=False)
            while listing:
                rv.extend(listing)
                if not delimiter:
                    marker = listing[-1]['name']
                else:
                    marker = listing[-1].get('name', listing[-1].get('subdir'))
                listing = self.get_container_list(account, container, marker,
                                                  end_marker, limit, prefix,
                                                  delimiter,
                                                  full_listing=False)
            return rv
        path = self.make_path(account, container)
        qs = 'format=json'
        if marker:
            qs += '&marker=%s' % quote(marker)
        if end_marker:
            qs += '&end_marker=%s' % quote(end_marker)
        if limit:
            qs += '&limit=%d' % limit
        if prefix:
            qs += '&prefix=%s' % quote(prefix)
        if delimiter:
            qs += '&delimiter=%s' % quote(delimiter)
        path += '?%s' % qs
        req = webob.Request.blank(path, environ={'REQUEST_METHOD': 'GET'})
        resp = self._handle_request(req)
        if resp.status_int < 200 or resp.status_int >= 300:
            return []  # TODO: distinguish between 404 and empty container
        if resp.status_int == 204:
            return []
        return json_loads(resp.body)

    def get_container_metadata(self, account, container):
        req = webob.Request.blank(
                  self.make_path(account, container),
                  environ={'REQUEST_METHOD': 'HEAD'})
        resp = self._handle_request(req)
        out = {}
        for k, v in resp.headers.iteritems():
            if k.lower().startswith('x-container-meta-'):
                out[k] = v
        return out
