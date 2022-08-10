#!/usr/bin/env python3

import sys
from ar import Peer, Wallet, DataItem
from ar.utils import create_tag
from bundlr import Node

class Stream:
    def __init__(self, metadata, peer):
        self.peer = peer
        self.start_block = self.block(metadata['start_block'])
        self.last_block = None
        self.offset = 0
        self.first = metadata['first']
        self.bundle_by_item = {}
        self.data_by_offset = {}
    def _scan_block(self, block):
        if type(block) in (int, str):
            block = self.block(block)
        for txid in block.txs:
            tx = Transaction.frombytes(peer.tx2(txid))
            tags = tx.tags
            if any((tag['name'].startswith(b'Bundle') for tag in tags)):
                try:
                    stream = self.peer.stream(txid)
                except ArweaveException as exc:
                    print(exc)
                    continue
                with stream:
                    header = ANS104Header.from_tags_stream(tags, stream)
                    offset = header.get_len_bytes()
                    for id, length in header.length_by_id.items():
                        def head_fetcher():
                            stream.seek(offset)
                            return ANS104DataItemHeader.from_tags_stream(tags, stream)
                        def full_fetcher():
                            stream.seek(offset)
                            return DataItem.from_tags_stream(tags, stream)
                        yield (txid, id, length, head_fetcher, full_fetcher)
                        offset += length
    def iterate_blocks(self, start_block, max_height):
        if type(start_block) in (int, str):
            start_block = self.block(start_block)
        yield start_block
        for height in range(start_block.height, max_height + 1):
            yield aelf.block(height)
    def iterate(self, start_block, max_height):
        if self.owner is None:
            for block in self.iterate_blocks(start_block, max_height):
                for bundle, id, length, head, full in self._scan_block(block):
                    if id == self.first:
                        self.owner = self.last_metadata.owner
                        full = full()
                        data = json.loads(full.data.decode())
                        self.data_by_offset[data['offset']] = data['txid']
                        break
        next_offset = 0
        for block in self.iterate_blocks(start_block, max_height):
            for id, length, head, full in self._scan_block(start_block):
                if next_offset in self.data_by_offset:
                    next_metadata = self.data_by_offset[next_offset]
                    next_data = next_metadata['txid']
                    if next_data] in self.bundle_by_item:
                        next_bundle = self.bundle_by_item[next_data['txid']]
                        stream = self.peer.stream(next_txid)
                        bundle = ANS104BundleHeader.fromstream(stream)
                        start, end = bundle.get_range(next_data)
                        stream.seek(start)
                        data = DataItem.fromstream(stream, end - start)
                        yield data
                        del self.data_by_offset[next_offset]
                        del self.bundle_by_item[next_data]
                head = head()
                if head.owner == self.owner:
                    self.bundle_by_item[full.header.id] = bundle
                if head.owner == self.owner and length < 50000:
                    full = full()
                    try:
                        data = json.loads(full.data.decode())
                        if data['first'] != self.first:
                            continue
                    except:
                        continue
                    self.data_by_offset[data['offset']] = data['txid']
                    del self.bundle_by_item[full.header.id]
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
