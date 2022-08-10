#!/usr/bin/env python3

import sys
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node

class Stream:
    def __init__(self, metadata, peer):
        self.peer = peer
        self.start_block = self.block(metadata['start_block'])
        self.block = self.start_block
        self.offset = 0
        self.first = metadata['first']
    def _scan_block(self):
        for tx in self.block.
    def next(self):
        self.block.height
        self.block.trans
    def block(self, id):
        if type(id) is str:
            Block.frombytes(self.peer.block2_hash(id))
        else:
            Block.frombytes(self.peer.block2_height(id))
        
{"first": "nJpmTWzfVon0eUk68RX1wq0gfeYL74gUyo9nVy70ID4", "prev": "co8nbKFhW1uMOPvyrWGuc2gKtWxv3Ki2a0dRN4F5o3Q", "txid": "9OW38VxDVZqYU70UCSqcIF2hTEbGs5MtKvMcYRY3gnw", "offset": 600000, "current_block": "sC-WALW67G_iK9jDY-J7Tdwa0sZvQ3HIcIYwcx0Hc4TuDugReRcSCHeXJnC1OOXb", "api_block": 993097, "start_block": "sC-WALW67G_iK9jDY-J7Tdwa0sZvQ3HIcIYwcx0Hc4TuDugReRcSCHeXJnC1OOXb"}

gw = Peer()
for fn in sys.argv[1:]:
    with open(fn) as fh:
        Stream(json.load(fh))
