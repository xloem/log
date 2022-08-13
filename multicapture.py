#!/usr/bin/env python3

import sys
import threading
import time
from collections import deque
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem, ArweaveNetworkException, logger
from ar.utils import create_tag
from bundlr import Node

import logging
logging.basicConfig(level=logging.DEBUG)

# indexes a balanced tree of past indices
class append_indices(list):
    def __init__(self, degree = 2, initial_indices = []):
        super().__init__(*initial_indices)
        self.degree = degree
        self.leaf_count = sum((leaf_count for leaf_count, _ in self))
    def append(self, last_indices_id):
        self.leaf_count += 1
        leaf_count = self.leaf_count
        idx = 0
        for idx, (sub_leaf_count, value) in enumerate(self):
            if sub_leaf_count * self.degree <= leaf_count:
                break
            leaf_count -= sub_leaf_count
            idx += 1 # to append if the loop falls through
        self[idx:] = [(leaf_count, last_indices_id)]

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


class Reader(threading.Thread):
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.start()
    def run(self):
        print('Capturing ...')
        capture_proc = Popen("./capture", stdout=PIPE)
        capture = capture_proc.stdout
        raws = []
        while running:
            raws.append(capture.read(100000))
            if Data.lock.acquire(blocking=False):
                Data.data.extend(raws)
                Data.lock.release()
                raws.clear()
                print(len(Data.data), 'captures queued while running')
        print('Finishing capturing')
        capture_proc.terminate()
        while True:
            raws.append(capture.read(100000))
            with self.lock:
                Data.data.extend(raws)
                raws.clear()
                print('Finishing capturing', len(self.data))
                if len(Data.data[-1]) < 100000:
                    break
        print('Capturing finished')

class Locationer:
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.start()
    def run(self):
        print('Locationing ...')
        while True:
            try:
                location_proc = Popen('termux-location', stdout=PIPE)
            except:
                print('Locationing failed.')
                break
            data = json.load(location_proc.stdout)


class Storer(threading.Thread):
    input_lock = threading.Lock()
    lock = threading.Lock()
    idx = 0
    output_idx = 0
    proc_idx = 0
    pool = set()
    output = deque()
    reader = Reader()
    exceptions = []
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.node = Node()
        self.pending = deque()
        self.start()
    def run(self):
        with self.input_lock:
            self.proc_idx = Storer.proc_idx
            Storer.proc_idx += 1
            self.pool.add(self)
            print(self.proc_idx, 'launching storing')
        try:
            while True:
                while len(self.pending) and self.pending[0][0] == self.output_idx:
                    next_idx, next_result = self.pending.popleft()
                    print(self.proc_idx, 'taking storing lock with the next item')
                    with self.lock:
                        print(self.proc_idx, 'took storing lock')
                        self.output.append(next_result)
                        Storer.output_idx += 1
                        print(self.proc_idx, 'stored', Storer.output_idx, 'index queue size =', len(self.output))
                #print(self.proc_idx, 'taking input_lock and Data.lock')
                with self.input_lock, Data.lock:
                    #print(self.proc_idx, 'took input_lock and Data.lock')
                    #with self.reader.lock:
                    #print(self.proc_idx, 'took reader_lock')
                    if len(Data.data) == 0:
                        if len(self.pending) or (len(self.pool) == 1 and self.reader.is_alive()):
                            continue
                        raise StopIteration()
                    data = Data.data.popleft()
                    if len(Data.data) > len(self.pool) * 2.25:
                        print(self.proc_idx, 'spawning new')
                        Storer()
    
                    idx = Storer.idx
                    Storer.idx += 1
                print(self.proc_idx, 'sending', idx)
                result = send(data)
                print(self.proc_idx, 'sent', idx)
                result['length'] = len(data)
                self.pending.append((idx, result))
        except StopIteration:
            print(self.proc_idx, 'finishing')
        except Exception as exc:
            print(self.proc_idx, 'raised exception', type(exc))
            with self.lock:
                self.exceptions.append(exc)
        self.pool.remove(self)
Storer()

first = None
start_block = None
prev = None
peer = Peer(retries=9999999)
offset = 0
indices = append_indices(3)
index_values = indices

current_block = peer.current_block()
last_time = time.time()
metadata = dict(api_block = None)

while True:
    try:
        with Storer.lock:
            if len(Storer.exceptions):
                for exception in Storer.exceptions:
                    raise exception
            data = [*Storer.output]
            Storer.output.clear()
            if not len(data):
                if not running and not len(Storer.pool):
                    print('index thread stopping no output left')
                    break
                try:
                    Storer.lock.release()
                    #print('no output to index')
                    time.sleep(0.1)
                finally:
                    Storer.lock.acquire()
                continue
            print('indexing', len(data), 'captures')
        if time.time() > last_time + 60:
            current_block = peer.current_block()
            last_time = time.time()
        metadata = dict(
            txid = [item['id'] for item in data],
            offset = offset,
            current_block = current_block['indep_hash'],
            api_block = [metadata['api_block'], *(item['block'] for item in data if 'block' in item)][-1],
            index = index_values
        )
        result = send(json.dumps(metadata).encode())
        prev = result['id']
        offset += sum((item['length'] for item in data))
        if first is None:
            first = prev
            start_block = current_block['indep_hash']
        indices.append(dict(dataitem=prev, current_block=current_block['indep_hash'], end_offset=offset))
    
        #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
        #eta = datetime.fromtimestamp(eta)
        index_values = [value for leaf_count, value in indices]
        with open(first, 'wt') as fh:
            json.dump(index_values[-1], fh)
        json.dump(index_values[-1], sys.stdout)
        sys.stdout.write('\n')
    except KeyboardInterrupt:
        if not running:
            break
        running = False
