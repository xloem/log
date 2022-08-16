#!/usr/bin/env python3

import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem, ArweaveNetworkException, logger
from ar.utils import create_tag
from bundlr import Node
from flat_tree import flat_tree

import logging
#logging.basicConfig(level=logging.DEBUG)

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

node = Node(timeout = 0.5)
def send(data, **tags):
    di = DataItem(data = data)
    di.header.tags = [
        create_tag(key, val, True)
        for key, val in tags.items()
    ]
    di.sign(wallet.rsa)
    while True:
        try:
            result = node.send_tx(di.tobytes())
            break
        except ArweaveNetworkException as exc:
            text, code, exc2, response = exc.args
            if code == 201: # already received
                return {'id': di.header.id}
            #pass
            logger.exception(text)
    return result

running = True

class Data:
    data = deque()
    lock = threading.Lock()
    @classmethod
    def append_needs_lk(cls, type, item):
        #if len(cls.data):
        #    entry = cls.data[-1].get(type)
        #    if entry is None:
        #        cls.data[-1][type] = [item]
        #    else:
        #        entry.append(item)
        #else:
        #    cls.data.append(dict(type=[item]))
        cls.data.append((type, item))
    @classmethod
    def extend_needs_lk(cls, type, items):
        #if len(cls.data):
        #    entry = cls.data[-1].setdefault(type, items)
        #    if entry is not items:
        #        entry.extend(items)
        #else:
        #    cls.data.append(dict(type=items))
        cls.data.extend(((type, item) for item in items))

class BinaryProcessStream(threading.Thread):
    def __init__(self, name, proc, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.name = name
        self.proc = proc
        self.start()
    def run(self):
        print(f'Beginning {self.name} ...')
        capture_proc = Popen(self.proc, stdout=PIPE)
        capture = capture_proc.stdout
        raws = []
        while running:
            raws.append(capture.read(100000))
            if Data.lock.acquire(blocking=False):
                Data.extend_needs_lk(self.name, raws)
                Data.lock.release()
                raws.clear()
                #print(len(Data.data), 'queued while running from', self.name)
        print(f'Finishing {self.name}ing')
        capture_proc.terminate()
        while True:
            raw = capture.read(100000)
            if len(raw) > 0:
                raws.appends(raw)
                with Data.lock:
                    Data.extend_needs_lk(self.name, raws)
                    raws.clear()
                    print(f'Finishing {self.name}', len(Data.data))
                    if len(Data.data[-1]) < 100000:
                        break
            elif not len(raws):
                break
        print(f'Finished {self.name}')

#class Reader(threading.Thread):
#    def __init__(self, *params, **kwparams):
#        super().__init__(*params, **kwparams)
#        self.start()
#    def run(self):
#        print('Capturing ...')
#        #capture_proc = Popen("./capture", stdout=PIPE)
#        capture_proc = Popen(('sh','-c','./capture | tee last_capture.log.bin'), stdout=PIPE)
#        capture = capture_proc.stdout
#        raws = []
#        while running:
#            raws.append(capture.read(100000))
#            if Data.lock.acquire(blocking=False):
#                Data.extend_needs_lk('capture', raws)
#                Data.lock.release()
#                raws.clear()
#                #print(len(Data.data), 'captures queued while running')
#        print('Finishing capturing')
#        capture_proc.terminate()
#        while True:
#            raw = capture.read(100000)
#            if len(raw) > 0:
#                raws.append(raw)
#                with Data.lock:
#                    Data.extend_needs_lk('capture', raws)
#                    raws.clear()
#                    print('Finishing capturing', len(Data.data))
#                    if len(Data.data[-1]) < 100000:
#                        break
#            elif not len(raws):
#                break
#        print('Capturing finished')

class Locationer:
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.start()
    def run(self):
        print('Locationing ...')
        raws = []
        last = None
        while running:
            try:
                location_proc = Popen('termux-location', stdout=PIPE)
            except:
                print('Locationing failed.')
                break
            raw = json.load(location_proc.stdout)
            raws.append(raw)
            if Data.lock.acquire(blocking=False):
                Data.append_needs_lk('location', raws)
                Data.lock.release()
                raws.clear()
        print('Locationing finished')


class Storer(threading.Thread):
    input_lock = threading.Lock()
    lock = threading.Lock()
    idx = 0
    output_idx = 0
    proc_idx = 0
    pool = set()
    output = defaultdict(deque)
    #reader = BinaryProcessStream('capture', './capture')
    readers = [
        BinaryProcessStream('capture', ('sh','-c','./capture | tee last_capture.log.bin')),
        Locationar(),
        BinaryProcessStream('logcat', 'logcat')
    ]
    exceptions = []
    logs = {}
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.node = Node()
        self.pending_input = deque()
        self.pending_output = deque()
        self.start()
    def print(self, *params):
        log = self.logs[self.proc_idx]
        if not len(log) or log[-1] != params:
            self.logs[self.proc_idx].append(params)
            print(*params)
    def run(self):
        with self.input_lock:
            self.proc_idx = Storer.proc_idx
            self.logs[self.proc_idx] = []
            Storer.proc_idx += 1
            self.pool.add(self)
            self.print(self.proc_idx, 'launching storing')
        last_log_time = 0
        last_log_time_2 = 0
        while True:
            try:
                while len(self.pending_input):
                    next_idx, next_type, next_data = self.pending_input[0]
                    self.print(self.proc_idx, 'sending', next_idx)
                    if type(data) is bytes:
                        result = send(next_data)
                        #print(self.proc_idx, 'sent', idx)
                        result['length'] = len(next_data)
                    elif type(data) is dict:
                        result = dict(id = next_data)
                    else:
                        raise AssertionError(f'unexpected content datatype {type(data)}: {channel}, {data}')
                    self.pending_output.append((next_idx, next_type, result))
                    self.pending_input.popleft()
                while len(self.pending_output) and self.pending_output[0][0] == self.output_idx:
                    next_idx, next_type, next_result = self.pending_output.popleft()
                    self.print(self.proc_idx, 'taking storing lock with the next item')
                    with self.lock:
                        #print(self.proc_idx, 'took storing lock')
                        self.output[next_type].append(next_result)
                        self.print(self.proc_idx, 'stored', Storer.output_idx, 'index queue size =', len(self.output))
                        Storer.output_idx += 1
                if len(self.pending_output) and self.pending_output[0][0] != self.output_idx:
                    self.print(self.pending_output[0][0], 'not queuing, waiting for', self.output_idx)
                    #print(self.logs)
                #print(self.proc_idx, 'taking input_lock and Data.lock')
                with self.input_lock, Data.lock:
                    if time.time() > last_log_time + 1:
                        last_log_time = time.time()
                        self.print(self.proc_idx, 'took input_lock and Data.lock')
                    #with self.reader.lock:
                    #print(self.proc_idx, 'took reader_lock')
                    if len(Data.data) == 0:
                        if len(self.pending_input) or len(self.pending_output):# or (len(self.pool) == 1 and self.reader.is_alive()):
                            continue
                        if time.time() > last_log_time_2 + 1:
                            last_log_time_2 = time.time()
                            self.print('Stopping', self.proc_idx, 'len(self.pending) =', len(self.pending_input), len(self.pending_output), '; len(self.pool) =', len(self.pool), '; self.reader.is_alive() =', self.reader.is_alive())
                        raise StopIteration()
                    channel, data = Data.data.popleft()
                    if len(Data.data) > len(self.pool) * (len(self.pending_input) + len(self.pending_output)):
                        self.print(self.proc_idx, 'spawning new; expected idx =', Storer.proc_idx)
                        Storer()
    
                    idx = Storer.idx
                    Storer.idx += 1
                self.pending_input.append((idx, channel, data))
                continue
            except StopIteration:
                self.print(self.proc_idx, 'finishing')
                pass
            except Exception as exc:
                self.print(self.proc_idx, 'raised exception', type(exc))
                with self.lock:
                    self.exceptions.append(exc)
            with Data.lock:
                if len(self.pool) == 1 and self.reader.is_alive():
                    #print(self.proc_idx, 'closed but reader still running, continuing anyway')
                    continue
                self.pool.remove(self)
                self.print('storers remaining:', *(storer.proc_idx for storer in self.pool))
                break
Storer()

first = None
start_block = None
prev = None
peer = Peer(retries=9999999)
offset = 0
indices = flat_tree(3) #append_indices(3)
#index_values = indices

current_block = peer.current_block()
last_time = time.time()

prev = None

while True:
    try:
        #print('taking Storer lock')
        with Storer.lock:
            if len(Storer.exceptions):
                for exception in Storer.exceptions:
                    raise exception
            data = Storer.output.copy()
            Storer.output.clear()
            if not len(data):
                if not running and not len(Storer.pool) and not Storer.reader.is_alive():
                    print('index thread stopping no output left')
                    break
                try:
                    Storer.lock.release()
                    #print('no output to index')
                    time.sleep(0.1)
                finally:
                    Storer.lock.acquire()
                continue
            print('indexing', len(data), 'captures, releasing Storer lock')
        if time.time() > last_time + 60:
            current_block = peer.current_block()
            last_time = time.time()
        # this could be a dict of lengths
        lengths = sum((capture['length'] for capture in data.get('capture', [])))
        datas = {
            type: dict(
                ditem = [item['id'] for item in items],
                length = sum((item['length'] for item in items))
            )
            for type, items in data.items()
        }
        indices.append(
            prev,
            lengths,
            dict(
                **datas,
                min_block = (current_block['height'], current_block['indep_hash']),
                api_block = max((item['block'] for items in data.values() for item in items if 'block' in item)) or None,
            )
        )
        result = send(json.dumps(indices.snap()).encode())
        prev = dict(
            ditem = [result['id']],
            min_block = (current_block['height'], current_block['indep_hash']),
            api_block = result['block']
        )
        if first is None:
            first = result['id']
            start_block = current_block['indep_hash']
    
        #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
        #eta = datetime.fromtimestamp(eta)
        #index_values = [value for leaf_count, value in indices]
        with open(first, 'wt') as fh:
            #json.dump(index_values[-1], fh)
            json.dump(prev, fh)
        #json.dump(index_values[-1], sys.stdout)
        json.dump(prev, sys.stdout)
        sys.stdout.write('\n')
    except KeyboardInterrupt:
        print('got a keyboard interrupt, setting running to false')
        if not running:
            break
        running = False
