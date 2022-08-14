#!/usr/bin/env python3

import sys, time
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node

print('warning: this script hopefully works but drops chunks due to waiting on network and not buffering input')

# indexes a balanced tree of past indices
class append_indices(list):
    def __init__(self, degree = 2, initial_indices = []):
        super().__init__(*initial_indices)
        self.degree = degree
        self.leaf_count = sum((leaf_count for type, data, size, leaf_count in self))
        self.size = sum((size for type, data, size, leaf_count in self))
    def append(self, last_indices_id, data, data_start, data_size):
        if last_indices_id is not None:
            node_leaf_count = self.leaf_count
            node_size = self.size
            node_start = 0
            idx = 0
            for idx, (branch_type, branch_data, branch_size, branch_leaf_count) in enumerate(self):
                if branch_leaf_count * self.degree <= node_leaf_count:
                    break
                node_leaf_count -= branch_leaf_count
                node_size -= branch_size
                node_start += branch_size
                idx += 1 # to append if the loop falls through
            self[idx:] = ((1, last_indices_id, node_start, node_size, node_leaf_count), (0, data, data_start, data_size, 1))
        else:
            asset len(self) == 0
            self[0:] = ((0, data, data_start, data_size, 1),)
        self.leaf_count += 1
        self.size += data_size

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

print('Capturing ...')
capture = Popen("./capture", stdout=PIPE).stdout

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

first = None
start_block = None
prev_indices_id = None
peer = Peer()
offset = 0
indices = append_indices(3)
#index_values = indices

current_block = peer.current_block()
last_time = time.time()

while True:
    raw = capture.read(100000)
    data = send(raw)
    if time.time() > last_time + 60:
        current_block = peer.current_block()
        last_time = time.time()
    indices.append(
        prev_indices_id,
        dict(
            capture = dict(
                ditem = [data['id']],
            ),
            min_block = (current_block['height'], current_block['indep_hash']),
            api_block = data['block'],
        ),
        data_start = 0,
        data_size = len(raw)
    )
    metadata = [(type, data, start, size) for type, data, start, size, *_ in indices]
    result = send(json.dumps(metadata).encode())
    prev_indices_id = dict(
        ditem = [result['id']],
        min_block = (current_block['height'], current_block['indep_hash']),
        api_block = result['block'],
    )
    #offset += len(raw)
    if first is None:
        first = prev_indices_id['ditem'][0]
        start_block = current_block['indep_hash']
    #indices.append(dict(dataitem=prev, current_block=current_block['indep_hash'])#, end_offset=offset), )

    #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
    #eta = datetime.fromtimestamp(eta)
    #index_values = [value for leaf_count, value in indices]
    with open(first, 'wt') as fh:
        #json.dump(index_values[-1], fh)
        json.dump(prev_indices_id, fh)
    #json.dump(index_values[-1], sys.stdout)
    json.dump(prev_indices_id, sys.stdout)
    sys.stdout.write('\n')
