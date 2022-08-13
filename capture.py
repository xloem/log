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
prev = None
peer = Peer()
offset = 0
indices = append_indices(3)
index_values = indices

current_block = peer.current_block()
last_time = time.time()

while True:
    raw = capture.read(100000)
    data = send(raw)
    if time.time() > last_time + 60:
        current_block = peer.current_block()
        last_time = time.time()
    metadata = dict(
        txid = data['id'],
        offset = offset,
        current_block = current_block['indep_hash'],
        api_block = data['block'],
        index = index_values
    )
    result = send(json.dumps(metadata).encode())
    prev = result['id']
    offset += len(raw)
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
