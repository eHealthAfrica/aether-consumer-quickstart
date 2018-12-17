#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

import contextlib
import enum
import errno
import os
import json
import signal
import sys
from time import sleep

from aet.consumer import KafkaConsumer
from blessings import Terminal
from kafka import TopicPartition

EXCLUDED_TOPICS = ['__confluent.support.metrics']


class timeout(contextlib.ContextDecorator):
    def __init__(self,
                 seconds,
                 *,
                 timeout_message=os.strerror(errno.ETIME),
                 suppress_timeout_errors=False):
        self.seconds = int(seconds)
        self.timeout_message = timeout_message
        self.suppress = bool(suppress_timeout_errors)

    def _timeout_handler(self, signum, frame):
        raise TimeoutError(self.timeout_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._timeout_handler)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
        if self.suppress and exc_type is TimeoutError:
            return True




def clear():
    print(t.clear())

def bold(obj):
    print(t.bold(obj))


def norm(obj):
    print(obj)


def error(obj):
    with t.location(int(t.width / 2 - len(obj) / 2), 0):
        print(t.black_on_white(obj))


def pjson(obj):
    print(t.bold(json.dumps(obj, indent=2)))


def wait():
    input("Press enter to continue")


class KafkaViewer(object):

    def __init__(self, interactive=True):
        if interactive:
            self.start()
        

    def start(self):
        self.killed = False
        signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)
        clear()
        self.topics()

    def ask(self, options):
        bold("Select an option from the list\n")
        for x, opt in enumerate(options, 1):
            line = "%s ) %s" % (x, opt)
            norm(line)
        while True:
            x = input("choices: ( %s ) : " %
                      ([x + 1 for x in range(len(options))]))
            try:
                res = options[int(x) - 1]
                return res
            except Exception as err:
                error("%s is not a valid option | %s" % (x, err))

    def get_consumer(self, quiet=False, topic=None):
        args = {}
        kafka_settings_path = os.environ['KAFKA_CONFIG']
        with open(kafka_settings_path) as f:
            args = json.load(f)
        if not quiet:
            clear()
            pjson(["Creating Consumer from %s args:" % kafka_settings_path, args])
        self.connect_consumer(**args)
        if not self.consumer:
            raise Exception('Could not connect to Kafka')
            sys.exit(1)
        if topic:
            self.consumer.subscribe(topic)

    # Just need something for the unit test to do as this is almost 100% integration
    def consumer_connected(self):
        try:
            if self.consumer:
                return True
            return False
        except AttributeError:
            return False

    # Just need something for integration tests as everything else needs lots of mocking.
    def connect_consumer(self, *args, **kwargs):
        try:
            self.consumer = KafkaConsumer(**kwargs)
        except Exception as err:
            self.consumer = None

    def kill(self, *args, **kwargs):
        self.killed = True

    def count_all_topic_messages(self, topic):  # individual messages
        subtotals = []
        self.get_consumer(True, topic)
        while not self.consumer.poll():
            sleep(0.1)
        self.consumer.seek_to_beginning()
        try:
            while True:
                messages = self.consumer.poll_and_deserialize(1000, 1000)
                if not messages:
                    return sum(subtotals)
                parts = [i for i in messages.keys()]
                for part in parts:
                    bundles = messages.get(part)
                    for bundle in bundles:
                        _msgs = bundle.get('messages')
                        subtotals.append(sum([1 for m in _msgs]))
        except Exception as err:
            raise err
        finally:
            self.consumer.close()
        
    def get_topic_size(self, topic):  # offsets
        self.get_consumer(True, topic)
        while not self.consumer.poll():
            sleep(0.1)
        self.consumer.seek_to_end()
        partitions = [TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)]
        end_offsets = self.consumer.end_offsets(partitions)
        self.consumer.close(autocommit=False)
        size = sum([i for i in end_offsets.values()])
        return size

    def print_topic_sizes(self, topics):
        clear()
        bold("Topic -> Count")
        for t in topics:
            size = self.count_all_topic_messages(t)
            norm("%s -> %s" % (t, size))
        wait()
        clear()
        return

    def topics(self):
        while True:
            topic_size = {}
            self.get_consumer(quiet=True)
            refresh_str = "-> Refresh Topics"
            detailed_str = "-> Get Real Message Counts for Topics"
            quit_str = "-> Exit KafkaViewer\n"
            bold('Fetching Topics')
            topics = sorted([i for i in self.consumer.topics() if not i in EXCLUDED_TOPICS])
            if not topics:
                bold("No topics available")
            for topic in topics:
                topic_size[topic] = self.get_topic_size(topic)
            clear()
            prompt_key = { "topic: %s {%s}" % (topic, topic_size[topic]) : topic for topic in topics }
            prompts = sorted(prompt_key.keys())
            prompts.extend([detailed_str, refresh_str, quit_str])
            bold("Choose a Topic to View")
            norm("-> topic {# of offsets in topic}\n")
            topic = self.ask(prompts)
            topic = prompt_key.get(topic) if topic in prompt_key else topic
            if topic is quit_str:
                return
            elif topic is refresh_str:
                clear()
                continue
            elif topic is detailed_str:
                self.print_topic_sizes(topics)
                continue

            self.get_consumer(topic=topic)
            self.consumer.seek_to_beginning()
            self.show_topic()

    def show_topic(self, batch_size=50):
        current = 0
        while True:
            messages = self.consumer.poll_and_deserialize(1000, batch_size)
            if not messages:
                norm("No more messages available!")
                return
            part = 0
            choices = [i for i in messages.keys()]
            if len(choices) > 1:
                bold("Choose a Parition to View")
                part = self.ask(choices)
                messages = messages.get(choices[part])
            else:
                messages = messages.get(choices[0])
            messages_read = self.view_messages(messages, batch_size, current)
            if not messages_read:
                return
            current += messages_read

    def view_messages(self, messages, batch_size, current):
        options = [
            "Next Message",
            "Skip forward to next package of messages",
            "View Current Schema",
            "Exit to List of Available Topics\n"
        ]
        pulled_size = sum([1 for message in messages for msg in message.get('messages') ])
        for x, message in enumerate(messages):
            if not message.get('messages'):
                bold('\nThe current settings return no messages\n')
            for y, msg in enumerate(message.get('messages')):
                norm("message #%s (%s of batch sized %s)" %
                 (current + 1, y + 1, pulled_size))
                pjson(msg)
                res = self.ask(options)
                idx = options.index(res)
                if idx == 1:
                    clear()
                    return pulled_size
                elif idx == 2:
                    pjson(message.get('schema'))
                    wait()
                elif idx == 3:
                    clear()
                    return False
                else:
                    clear()
                current +=1
    
    def view_topics(self):
        self.get_consumer(quiet=True)
        return sorted([i for i in self.consumer.topics() if not i in EXCLUDED_TOPICS])


def get_arg(pos, args):
    try:
        if len(args) > pos:
            return args[pos]
    except ValueError:
        print(f'{args[pos]} was a bad value for position {pos}')
        return None

class CMD(enum.Enum):
    LIST = 1
    COUNT = 2


if __name__ == "__main__":
    args = sys.argv
    try:
        cmd = CMD[get_arg(1, args)]
        viewer = KafkaViewer(interactive=False)
        if cmd is CMD.LIST:
            print(viewer.view_topics())
        elif cmd is CMD.COUNT:
            topic = get_arg(2, args)
            if not topic:
                raise ValueError('No topic specified.')
            count = viewer.count_all_topic_messages(topic)
            print("%s individual messages in topic: %s" % (topic, count))

    except KeyError:
        global t
        t = Terminal()
        viewer = KafkaViewer()
