#!/usr/bin/env python3

import sys
import threading
import time
from collections import deque
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node

# indexes a balanced tree of past indices
class append_indices(list):
    def __init__(self, degree = 2, initial_indices = []):
        super().__init__(*initial_indices)
        self.degree = degree
        self.leaf_count = 0
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

node = Node()
def send(data, **tags):
    di = DataItem(data = data)
    di.header.tags = [
        create_tag(key, val, True)
        for key, val in tags.items()
    ]
    di.sign(wallet.rsa)
    result = node.send_tx(di.tobytes())
    return result

running = True

class Reader(threading.Thread):
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.data = deque()
        self.lock = threading.Lock()
        self.start()
    def run(self):
        print('Capturing ...')
        capture = Popen("./capture", stdout=PIPE).stdout
        raws = []
        while running:
            raws.append(capture.read(100000))
            if self.lock.acquire(blocking=False):
                self.data.extend(raws)
                self.lock.release()
                raws.clear()
                print(len(self.data), 'captured')
        with self.lock:
            self.data.extend(raws)
            raws.clear()

class Storer(threading.Thread):
    input_lock = threading.Lock()
    lock = threading.Lock()
    idx = 0
    output_idx = 0
    proc_idx = 0
    pool = set()
    output = deque()
    reader = Reader()
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        with self.input_lock:
            self.proc_idx = Storer.proc_idx
            Sorer.proc_idx += 1
            self.pool.add(self)
            print(self.proc_idx, 'launching storing')
        self.node = Node()
        self.pending = deque()
        self.start()
    def run(self):
        while True:
            while len(self.pending) and self.pending[0][0] == self.output_idx:
                next_idx, next_result = self.pending.popleft()
                with self.lock:
                    self.output.append(next_result)
                    Storer.output_idx += 1
                    print('stored', Storer.output_idx)
            with self.input_lock:
                with self.reader.lock:
                    if len(self.reader.data) == 0:
                        if len(self.pending) or (len(self.pool) == 1 and running):
                            continue
                        self.pool.remove(self)
                        return
                    data = self.reader.data.popleft()
                    if len(self.reader.data) > len(self.pool) * 2.25:
                        Storer()
                idx = self.idx
                self.idx += 1
            result = send(data)
            result['length'] = len(data)
            self.pending.append((idx, result))
Storer()

first = None
start_block = None
prev = None
peer = Peer()
offset = 0
indices = append_indices(3)
index_values = indices

while True:
    try:
        with Storer.lock:
            data = [*Storer.output]
            Storer.output.clear()
            if not len(data):
                if not running and not len(Storer.pool):
                    print('index thread stopping no output left')
                    break
                print('no output to index')
                time.sleep(0.1)
                continue
            print('indexing', len(data), 'captures')
        current_block = peer.current_block()['indep_hash']
        metadata = dict(
            txid = [item['id'] for item in data],
            offset = offset,
            current_block = current_block,
            api_block = data[-1]['block'],
            index = index_values
        )
        result = send(json.dumps(metadata).encode())
        prev = result['id']
        offset += sum((item['length'] for item in data))
        if first is None:
            first = prev
            start_block = current_block
        indices.append(dict(dataitem=prev, current_block=current_block, end_offset=offset))
    
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
