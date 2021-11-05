#!/usr/bin/python3
import requests
import swiftclient
import json
import config as cfg
import math
from colorama import Fore, Style
import time

class ObjectStore():
    def __init__(self):
        self.conn = self.connect_swift()
        self.token = self.get_token()
        self.metadata = {}
        pass

    def connect_swift(self):
        url = 'http://%s:%d/auth/1.0' % (cfg.rgw_host, cfg.rgw_port)
        conn = swiftclient.Connection(
            user=cfg.swift_user,
            key=cfg.swift_key,
            authurl=url)
        return conn

    def get_token(self):
        token = self.conn.get_auth()[1]
        return token

    def add_to_metadata_tree(self, metadata, name, size):
        path_element = name.split('/')
        meta_ptr = metadata
        for element in path_element:
            if element not in meta_ptr:
                meta_ptr[element] = {'objs': {}, 'size': 0}
            meta_ptr[element]['size'] += size
            meta_ptr = meta_ptr[element]['objs']

    def load_metadata(self):
        metadata = {}
        return metadata
        metadata_swift = self.conn.get_container(cfg.bucket_name)[1]
        for data in metadata_swift:
            full_name = data['name']

            if full_name.endswith('_SUCCESS'): continue;
            self.add_to_metadata_tree(metadata, data['name'], data['bytes'])
        self.metadata = metadata
        return metadata

    def fetch_object_partial(self, bucket_name, obj_name, ofs_s, ofs_e):
        url = 'http://%s:%d/swift/v1/%s/%s' % (cfg.rgw_host, cfg.rgw_port, bucket_name, obj_name)
        headers = {"range": "bytes=%d-%d" % (ofs_s, ofs_e),
                   "X-Auth-Token": self.token}
        r = requests.get(url, headers=headers)

        if r.status_code not in [200, 201, 204, 206]:
            raise NameError(Fore.LIGHTRED_EX + '\t\tPrefetch %s/%s  %d-%d, failed with statuscode: %d'%(bucket_name, obj_name, ofs_s, ofs_e, r.status_code) + Style.RESET_ALL )
        print(r.status_code)


    def get_dataset_metadata(self, path, wave=-1, stride=0):
        path_element = path.split('/')
        meta_ptr = self.metadata
        for element in path_element:
            if element not in meta_ptr:
                return -1  # means could not prefetch this input
            meta_ptr = meta_ptr[element]['objs']
        return meta_ptr

