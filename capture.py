#!/usr/bin/env python3

import sys, time
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node
# indexes a balanced tree of past indices
from flat_tree import flat_tree

print('warning: this script hopefully works but drops chunks due to waiting on network and not buffering input')

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

print('Capturing ...')
#capture = Popen("./capture", stdout=PIPE).stdout
capture = Popen(('sh','-c','./capture | tee last_capture.log.bin'), stdout=PIPE).stdout

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
indices = flat_tree(3) #append_indices(3)
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
        len(raw),
        dict(
            capture = dict(
                ditem = [data['id']],
            ),
            min_block = (current_block['height'], current_block['indep_hash']),
            #api_block = data['block'],
            api_timestamp = data['timestamp'],
        ),
    )
    metadata = indices.snap()#[(type, data, start, size) for type, data, start, size, *_ in indices]
    result = send(json.dumps(metadata).encode())
    prev_indices_id = dict(
        ditem = [result['id']],
        min_block = (current_block['height'], current_block['indep_hash']),
        #api_block = result['block'],
        api_timestamp = result['timestamp'],
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
