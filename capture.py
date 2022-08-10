#!/usr/bin/env python3

import sys
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node

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
while True:
    raw = capture.read(100000)
    data = send(raw)
    current_block = peer.current_block()['indep_hash']
    metadata = dict(
        first = first,
        prev = prev,
        txid = data['id'],
        offset = offset,
        current_block = current_block,
        api_block = data['block'],
        start_block = start_block
    )
    result = send(json.dumps(metadata).encode())
    offset += len(raw)
    prev = result['id']
    if first is None:
        first = prev
        start_block = current_block

    #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
    #eta = datetime.fromtimestamp(eta)
    with open(first, 'wt') as fh:
        json.dump(metadata, fh)
    json.dump(metadata, sys.stdout)
    sys.stdout.write('\n')
