#!/usr/bin/env python

import iptools
import datetime

import unittest
from slogging import access_processor


class TestAccessProcessorSpeed(unittest.TestCase):
    def test_CIDR_speed(self):
        line = 'Sep 16 20:00:02 srv testsrv 192.%s.119.%s - ' \
               '16/Sep/2012/20/00/02 GET /v1/a/c/o HTTP/1.0 ' \
               '200 - StaticWeb - - 17005 - txn - 0.0095 -'
        ips1 = iptools.IpRangeList(*[x.strip() for x in
                                   '127.0.0.1,192.168/16,10/24'.split(',')
                                   if x.strip()])
        ips2 = iptools.IpRangeList(*[x.strip() for x in
                                   '172.168/16,10/30'.split(',')
                                   if x.strip()])
        ips3 = iptools.IpRangeList(*[x.strip() for x in
                                   '127.0.0.1,11/24'.split(',')
                                   if x.strip()])

        print "Original IPTools code start : "+str(datetime.datetime.utcnow())
        hit = 0
        for n in range(255):
            for a in range(255):
                stream = line % (n, a)
                data = stream.split(" ")
                if data[5] in ips1 or data[5] in ips2 or data[5] in ips3:
                    hit += 1
        if hit != 255:
            print "something went wrong. Expected 255 hits got : %s" % hit
        print "Original IPTools code finish : "+str(datetime.datetime.utcnow())

        # now, let's check the speed with pure dicts
        dict1 = set(k for k in
                     iptools.IpRangeList(*[x.strip() for x in
                     '127.0.0.1,192.168/16,10/24'.split(',') if x.strip()]))
        dict2 = set(k for k in
                     iptools.IpRangeList(*[x.strip() for x in
                     '172.168/16,10/30'.split(',') if x.strip()]))
        dict3 = set(k for k in
                     iptools.IpRangeList(*[x.strip() for x in
                     '127.0.0.1,11/24'.split(',') if x.strip()]))

        print "New IPTools code finish : "+str(datetime.datetime.utcnow())
        hit = 0
        for n in range(255):
            for a in range(255):
                stream = line % (n, a)
                data = stream.split(" ")
                if data[5] in dict1 or data[5] in dict2 or data[5] in dict3:
                    hit += 1
        if hit != 255:
            print "something went wrong. Expected 255 hits got : %s" % hit
        print "New IPTools code finish : "+str(datetime.datetime.utcnow())
