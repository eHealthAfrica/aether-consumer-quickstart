#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os

CONSUMER_CONFIG = None
KAFKA_CONFIG = None


class Settings(dict):
    # A container for our settings
    def __init__(self, file_path=None, alias=None, exclude=None):
        if not exclude:
            self.exclude = []
        else:
            self.exclude = exclude
        self.alias = alias
        self.load(file_path)

    def get(self, key, default=None):
        if self.exclude is not None and key in self.exclude:
            return default
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key):
        if self.alias and key in self.alias:
            key = self.alias.get(key)
        result = os.environ.get(key.upper())
        if result is None:
            result = super().__getitem__(key)

        return result

    def copy(self):
        keys = [k for k in self.keys() if k not in self.exclude]
        for key in self.alias:
            keys.append(key)
        return {k: self.get(k) for k in keys}

    def load(self, path):
        with open(path) as f:
            obj = json.load(f)
            for k in obj:
                self[k] = obj.get(k)


def load_config():
    CONSUMER_CONFIG_PATH = os.environ.get('CONSUMER_CONFIG_PATH')
    KAFKA_CONFIG_PATH = os.environ.get('KAFKA_CONFIG_PATH')
    global CONSUMER_CONFIG
    CONSUMER_CONFIG = Settings(file_path=CONSUMER_CONFIG_PATH)

    consumer_required = []

    for i in consumer_required:
        if not CONSUMER_CONFIG.get(i):
            raise ValueError(f'{i} is a required entry in the Consumer config or the environment')

    global KAFKA_CONFIG
    KAFKA_CONFIG = Settings(
        file_path=KAFKA_CONFIG_PATH,
        alias={'bootstrap_servers': 'kafka_url'},
        exclude=['kafka_url']
    )
    kafka_required = [
        'KAFKA_URL'
    ]
    for i in kafka_required:
        if not KAFKA_CONFIG.get(i):
            raise ValueError(f'{i} is a required entry in the Kafka config or the environment')


def get_KAFKA_CONFIG():
    return KAFKA_CONFIG


def get_CONSUMER_CONFIG():
    return CONSUMER_CONFIG


load_config()
