#!/usr/bin/env python3

import logging
import sys
import json
from ar import Block, Transaction, Peer, DataItem, ANS104BundleHeader, ANS104DataItemHeader
try:
    from tqdm import tqdm
except:
    def tqdm(iter, *params, **kwparams):
        yield from iter

#logging.basicConfig(level = logging.DEBUG)

class Stream:
    def __init__(self, metadata, peer):
        self.peer = peer
        self.height_cache = {}
        self.bundle_cache = {}
        self.cached_bundle = None
        if 'index' in metadata:
            # full metadata for an ending range
            self.tail = metadata
        elif 'end_offset' in metadata:
            self.tail = self.dataitem_json(metadata['dataitem'], metadata['current_block'])
    def iterate(self):
        offset = 0
        indices = [self.tail]
        while len(indices):
            while offset < indices[-1]['offset']:
                for index in indices[-1]['index']:
                    if offset < index['end_offset']:
                        indices.append(self.dataitem_json(index['dataitem'], index['current_block']))
                        break
            index = indices.pop()
            assert offset == index['offset']
            header, stream, length = self.dataitem(index['txid'], index['current_block'])
            yield index, header, stream, length
            offset += length
    def fetch_block(self, block):
        if type(block) is str:
            block = self.peer.block2_hash(block)
        elif type(block) is int:
            block = self.peer.block2_height(block)
        else:
            return block
        return Block.frombytes(block)
    def _cache_block(self, block):
        block = self.fetch_block(block)
        bundles = []
        for txid in tqdm(block.txs, unit='tx', desc=f'Caching {block.height}'):
            tags = self.peer.tx_tags(txid)
            if any((tag['name'].startswith(b'Bundle') for tag in tags)):
                bundles.append(txid)
        self.bundle_cache[block.height] = bundles
        self.height_cache[block.indep_hash] = block.height
        return block.height, bundles
    def block_bundles(self, block):
        if type(block) is str:
            block = self.block_height(block)
        bundles = self.bundle_cache.get(block)
        if bundles is None:
            self._cache_block(block)
            bundles = self.bundle_cache[block]
        return bundles
    def block_height(self, block):
        height = self.height_cache.get(block)
        if height is None:
            self._cache_block(block)
            height = self.height_cache[block]
        return height
    def dataitem(self, id, preceding_block):
        try:
            header, stream = self.cached_bundle
            start, end = header.get_range(id)
            stream.seek(start)
            return ANS104DataItemHeader.fromstream(stream), stream, end - stream.tell()
        except:
            pass
        preceding_height = self.block_height(preceding_block)
        for height in range(preceding_height + 1, self.tail['api_block'] + 1):
            for bundle in self.block_bundles(height):
                try:
                    stream = self.peer.stream(bundle)
                except ArweaveException as exc:
                    print(exc)
                    continue
                stream.__enter__()
                header = ANS104BundleHeader.fromstream(stream)
                if id in header.length_by_id:
                    if self.cached_bundle is not None:
                        old_header, old_stream = self.cached_bundle
                        old_stream.__exit__(None, None)
                    self.cached_bundle = header, stream
                    start, end = header.get_range(id)
                    stream.seek(start)
                    return ANS104DataItemHeader.fromstream(stream), stream, end - stream.tell()
                else:
                    stream.__exit__(None, None)
        raise KeyError(id, preceding_block)
    def dataitem_json(self, id, preceding_block):
        header, stream, length = self.dataitem(id, preceding_block)
        return json.loads(stream.read(length))

for fn in sys.argv[1:]:
    with open(fn) as fh:
        stream = Stream(json.load(fh), Peer())
    for metadata, header, stream, length in stream.iterate():
        sys.stdout.buffer.write(stream.read(length))
