#!/usr/bin/env python3

import sys, time
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node
# indexes a balanced tree of past indices
from flat_tree import flat_tree, __version__ as flat_tree_version

print('warning: this script hopefully works but drops chunks due to waiting on network and not buffering input')

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

print('Capturing ...')
#capture = Popen("./capture", stdout=PIPE).stdout
capture = Popen(('sh','-c','./capture | tee last_capture.log.bin'), stdout=PIPE).stdout

class BundlrStorage:
    def __init__(self, **tags):
        self.peer = Peer()
        self.node = Node()
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
        result = self.send(json.dumps(metadata).encode())
        return dict(
            ditem = [result['id']],
            min_block = (self.current_block['height'], self.current_block['indep_hash']),
            #api_block = result['block'],
            api_timestamp = result['timestamp'],
        )
    def store_data(self, raw):
        data = self.send(raw)
        return dict(
            capture = dict(
                ditem = [data['id']],
            ),
            min_block = (self.current_block['height'], self.current_block['indep_hash']),
            #api_block = data_array[-1]['block'],
            api_timestamp = data_array[-1]['timestamp'],
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

while True:
    raw = capture.read(100000)
    if flat_tree_version in ('0.0.0', '0.0.1'): # took an index id
        indices.append(
            prev_indices_id,
            len(raw),
            bundlrstorage.store_data(raw)
        )
        metadata = indices.snap()#[(type, data, start, size) for type, data, start, size, *_ in indices]
        prev_indices_id = bundlrstorage.store_index(metadata)
    else: # took a storage object
        indices.append(
            len(raw),
            bundlrstorage.store_data(raw)
        )
        prev_indices_id = indices.locator
    #offset += len(raw)
    if first is None:
        first = prev_indices_id['ditem'][0]
        start_block = bundlrstorage.current_block['indep_hash']
    #indices.append(dict(dataitem=prev, current_block=current_block['indep_hash'])#, end_offset=offset), )

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
