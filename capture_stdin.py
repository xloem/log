#!/usr/bin/env python3

import time
import logging
import concurrent.futures
last_post_time = time.time()

from datetime import datetime
from subprocess import Popen, PIPE
import json
import ar
import hashlib
from ar import Peer, Wallet, DataItem, ArweaveNetworkException
from ar.utils import create_tag
from bundlr import Node
# indexes a balanced tree of past indices
from flat_tree import flat_tree, __version__ as flat_tree_version

#print('warning: this script hopefully works but drops chunks due to waiting on network and not buffering input')
import nonblocking_stream_queue as nonblocking

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

print('Capturing ...')
#capture = Popen("./capture", stdout=PIPE).stdout
#capture = Popen(('sh','-c','./capture | tee last_capture.log.bin'), stdout=PIPE).stdout
import sys, os
if len(sys.argv) <= 1:
    fh = sys.stdin.buffer
else:
    try:
        fh = os.fdopen(int(sys.argv[1]), 'rb')
    except:
        fh = open(sys.argv[1], 'rb')
reader = nonblocking.Reader(
    fh,
    max_size=100000,
    lines=False,
    #lines=True,
    max_count=16*1024,#256, #1024, # max number queued
    drop_timeout=None, #0, # max time to wait adding to queue when full (waits forever if None)
    drop_older=True,
    pre_cb=lambda: time.time(),
    post_cb=lambda tuple: (*tuple, time.time()),
    verbose=True,
)
max_at_once = 32#64
#capture = sys.stdin.buffer

class BundlrStorage:
    def __init__(self, **tags):
        self.peer = Peer('https://ar-io.dev', timeout=240)#)
        self.node = Node(timeout=240)#60)
        self.tags = tags
        self._current_block = self.peer.current_block()
        self._last_block_time = time.time()
    @property
    def current_block(self):
        now = time.time()
        if now > self._last_block_time + 60:
            try:
                self._current_block = self.peer.current_block()
                self._last_block_time = now
            except:
                pass
        return self._current_block
    def store_index(self, metadata):
        data = json.dumps(metadata).encode()
        result = self.send(data)
        confirmation = self.send(json.dumps(result).encode())
        sha256 = hashlib.sha256()
        sha256.update(data)
        sha256 = sha256.hexdigest()
        blake2b = hashlib.blake2b()
        blake2b.update(data)
        blake2b = blake2b.hexdigest()
        return dict(
            ditem = [result['id']],
            min_block = (self.current_block['height'], self.current_block['indep_hash']),
            #api_block = result['block'],
            rcpt = confirmation['id'],
            sha256 = sha256,
            blake2b = blake2b,
        )
    def store_data(self, raws):
        global last_post_time # so it can start prior to imports
        global dropped_ct, dropped_size # for quick implementation
        #data_array = []
        #for offset in range(0,len(raw),100000):
        #    data_array.append(send(raw[offset:offset+100000]))
        #data_array = [self.send(raw) for pre_time, raw, post_time in raws]
        data_array = list(concurrent.futures.ThreadPoolExecutor(max_workers=4).map(self.send, [raw for pre_time, raw, post_time in raws]))
        confirmation = self.send(json.dumps(data_array).encode())
        sha256 = hashlib.sha256()
        for pre, raw, post in raws:
          sha256.update(raw)
        sha256 = sha256.hexdigest()
        blake2b = hashlib.blake2b()
        for pre, raw, post in raws:
          blake2b.update(raw)
        blake2b = blake2b.hexdigest()
        return dict(
            capture = dict(
                ditem = [data['id'] for data in data_array],
                time = [pre_time for pre_time, raw, post_time in raws],
            ),
            min_block = (self.current_block['height'], self.current_block['indep_hash']),
            #api_block = data_array[-1]['block'],
            rcpt = confirmation['id'],
            sha256 = sha256,
            blake2b = blake2b,
            dropped = dict(
                count = dropped_ct,
                size = dropped_size,
                time = last_post_time,
            ) if dropped_ct else None,
        )
    def send(self, data, **tags):
        di = ar.DataItem(data = data)
        di.header.tags = [
            create_tag(key, val, True)
            for key, val in {**self.tags, **tags}.items()
        ]
        di.sign(wallet.rsa)
        while True:
            try:
                start = time.time()
                result = self.node.send_tx(di.tobytes())
                break
            except ar.ArweaveNetworkException as exc:
                message, status_code, cause, response = exc.args
                if status_code == 201: # transaction already received
                    return dict(
                        id = di.header.id,
                        timestamp = f'code 201 (already received) around {start*1000}'
                    )
                logging.exception(exc)
                print(exc, file=sys.stderr)
                continue
            except Exception as exc:
                print(exc, file=sys.stderr)
                continue
        return result

bundlrstorage = BundlrStorage()
first = None
start_block = None
prev_indices_id = None
offset = 0
if flat_tree_version in ('0.0.0', '0.0.1'): # took an index id
    indices = flat_tree(3) #append_indices(3)
else: # took a storage object
    indices = flat_tree(bundlrstorage, 3)
#index_values = indices

#dump = open('dump.bin', 'wb')
while reader.block():
    #raw = capture.read(100000*16)#100000)
    #reader.block()
    with reader:
        dropped_ct, dropped_size = reader.dropped(reset = True)
        raws = reader.read_many(max_at_once)
    sys.stderr.write(f'Read {len(raws)} data chunks\n')
    if dropped_ct:
        sys.stderr.write(f'Dropped {dropped_size} bytes from {dropped_ct} reads at {last_post_time}\n')
    sys.stderr.flush()
    #for start_time, raw, end_time in raws:
    #    dump.write(raw)
    #if len(raw) == 0:
    #    break
    if flat_tree_version in ('0.0.0', '0.0.1'): # took an index id
        indices.append(
            prev_indices_id,
            sum((len(raw) for pre_time, raw, post_time in raws)),
            bundlrstorage.store_data(raws)
        )
        metadata = indices.snap()#[(type, data, start, size) for type, data, start, size, *_ in indices]
        prev_indices_id = bundlrstorage.store_index(metadata)
    else: # took a storage object
        indices.append(
            sum((len(raw) for pre_time, raw, post_time in raws)),
            bundlrstorage.store_data(raws)
        )
        prev_indices_id = indices.locator
    last_pre_time, last_raw, last_post_time = raws[-1]
    #offset += len(raw)
    #indices.append(dict(dataitem=prev, current_block=current_block['indep_hash'])#, end_offset=offset), )

    if first is None:
        first = prev_indices_id['ditem'][0]
        start_block = bundlrstorage.current_block['indep_hash']

    #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
    #eta = datetime.fromtimestamp(eta)
    #index_values = [value for leaf_count, value in indices]
    try:
        with open(first, 'wt') as fh:
            #json.dump(index_values[-1], fh)
            json.dump(prev_indices_id, fh)
    except Exception as exc:
        print(exc, file=sys.stderr)
    #json.dump(index_values[-1], sys.stdout)
    json.dump(prev_indices_id, sys.stdout)
    sys.stdout.write('\n')
