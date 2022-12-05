#!/usr/bin/env python3

import datetime
import itertools, logging, sys, time
import json
from flat_tree import flat_tree
from ar import Block, Transaction, Peer, DataItem, ANS104BundleHeader, ANS104DataItemHeader, ArweaveException, ArweaveNetworkException, logger
import ar.utils
try:
    from tqdm import tqdm
except:
    logging.warning('tqdm not found, no progress output')
    def tqdm(iter, *params, **kwparams):
        yield from iter

logging.warning('Note: this script downloads without verifying data integrity. Not that hard to add integrity checks into pyarweave\'s peer stream class.')

#logging.basicConfig(level = logging.DEBUG)

class Stream:
    def __init__(self, metadata, peer, follow_owner = True):
        self.peer = peer
        self.channels = set()
        self.height_cache = {}
        self.bundle_cache = {}
        self.cached_bundle = None
        self.follow_owner = follow_owner
        self.tail_height = 0
        if type(metadata) is list:
            # full metadata for an ending range
            self.tail = metadata
            guess_owner_metadata = [item for item in self.tail if item[0] > 0][-1][1] # find endmost non-leaf index
        elif 'ditem' in metadata:
            self.tail = sum((self.dataitem_json(ditem, metadata['min_block']) for ditem in metadata['ditem']), start=[])
            guess_owner_metadata = metadata
        else:
            raise AssertionError('unexpected metadata structure', metadata)
        if follow_owner is True:
            ditem_header, ditem_stream, ditem_size = self.dataitem(guess_owner_metadata['ditem'][-1], guess_owner_metadata['min_block'])
            self.follow_owner = ditem_header.owner
            logger.warning(f'following not implemented yet, but guessing owner to follow as {self.follow_owner} !')
        else:
            self.follow_owner = follow_owner
    def __len__(self):
        #self.poll()
        return sum((size for leaf_count, data, start, size in self.tail))
    #def poll(self):
    #   ## -> check heights >= self.tail_height [note: code could be organized/optimized with content of dataitem()]
    #    if self.follow_owner and peer.height() > 
    def iterate(self):
        # this function is the guts of a class that wraps a tree root record
        # indexing binary data on a blockchain. it is intended to yield the
        # chunks in order when called.

        comparison = flat_tree(degree=3)

        # the number of bytes that have been yielded, increased every chunk
        stream_output_offset = 0

        # the size of all the chunks: the sum of the sizes of each child node
        total_size = len(self)

        # for debugging: tracks nodes that should only be visited once, to check this
        visited = {}

        # a stack to perform a depth-first enumeration of the tree
        # atm, the child offset is not tracked as inner nodes are traversed.
        # instead, the global offset is tracked, and children are enumerated
        # again when backtracking, under the idea that total time is still
        # O(log n)
        #           index      stream offset  index offset  region size
        indices = [(self.tail, 0,             0,            total_size)]

        while len(indices):
            index, index_offset, index_start, index_size = indices[-1]
            index_offset_in_stream = index_offset - index_start
            expected_stream_output_offset = index_offset + index_size
            for leaf_count, index, index_substart, index_subsize, *_ in index:
                if index_offset_in_stream == stream_output_offset and index_subsize > 0:
                    if leaf_count > 0:
                        for ditem in index['ditem']:
                            assert ditem not in visited
                            visited[ditem] = indices.copy()
                        ditem = sum((self.dataitem_json(ditem, index['min_block']) for ditem in index['ditem']), start=[])
                        # after appending, it is not processing correct region
                        #print('adding', ditem)
                        indices.append((ditem, index_offset_in_stream, index_substart, index_subsize))
                        break
                    else:
                        comparison.append(comparison.leaf_count, index_subsize, index)
                        #print('yielding', index)
                        for channel_name, channel_data in index.items():
                            if type(channel_data) is dict and 'ditem' in channel_data:
                                sys.stderr.write(f'yielding {channel_name} @ {stream_output_offset}\n')
                                self.channels.add(channel_name)
                                length_sum = 0
                                if 'time' in channel_data:
                                    times = channel_data['time']
                                else:
                                    times = [None for ditem in channel_data['ditem']]
                                for time, ditem in zip(times, channel_data['ditem']):
                                    if type(ditem) is not dict:
                                        assert ditem not in visited
                                        visited[ditem] = indices.copy()
                                        header, stream, length = self.dataitem(ditem, index['min_block'])
                                    else:
                                        header, stream, length = (None, ditem, 1)
                                    length_sum += length
                                    #assert length > 0
                                    yield index, channel_name, time, header, stream, length
                                    #print(index_offset_in_stream, stream_output_offset, index['capture']['ditem'])
                                    if channel_name == 'capture':
                                        stream_output_offset += length
                                if channel_name == 'capture':
                                    assert index_subsize == length_sum
                else:
                    #print('skipping', index)
                    assert index_offset_in_stream <= stream_output_offset
                index_offset_in_stream += index_subsize
            else:
                #print('popping')
                assert stream_output_offset == expected_stream_output_offset
                indices.pop()
        assert stream_output_offset == total_size
        assert index_offset_in_stream == total_size
    def fetch_block(self, block):
        if type(block) is str:
            block = self.peer.block2_hash(block)
        elif type(block) is int:
            block = self.peer.block2_height(block)
        else:
            return block
        return Block.frombytes(block)
    def _txs2bundles(self, txs, label, unconfirmed=False):
        bundles = []
        with tqdm(txs, unit='tx', desc=label) as items:
            for txid in items:
                if unconfirmed:
                    tags = Transaction.frombytes(self.peer.unconfirmed_tx2(txid)).tags
                else:
                    tags = self.peer.tx_tags(txid)
                if any((ar.utils.b64dec_if_not_bytes(tag['name']) in (b'Bundle-Format', b'Bundle-Version') for tag in tags)):
                    bundles.append(txid)
        return bundles
    def _cache_block(self, block):
        block = self.fetch_block(block)
        bundles = self._txs2bundles(block.txs, f'Caching {block.height}')
        self.bundle_cache[block.height] = bundles
        self.height_cache[block.indep_hash] = block.height
        return block.height, bundles
    def block_bundles(self, block):
        if block is None:
            pending = self.peer.tx_pending()
            return self._txs2bundles(pending, f'{len(pending)} pending (central gateway)', unconfirmed = True)
        if type(block) is str:
            block = self.block_height(block)
        bundles = self.bundle_cache.get(block)
        if bundles is None:
            #current_height = self.peer.height()
            #if block > current_height:
            #     raise KeyError(f'block {block} does not exist yet')
            self._cache_block(block)
            bundles = self.bundle_cache[block]
        return bundles
    def recache_blocks_from(self, min_height):
        extra_heights = [height for height in self.bundle_cache if height >= min_height]
        for height in extra_heights:
            del self.bundle_cache[height]
    def block_height(self, block):
        if type(block) is list:
            for block in block:
                if type(block) is int:
                    return block
        height = self.height_cache.get(block)
        if height is None:
            self._cache_block(block)
            height = self.height_cache[block]
        return height
    def dataitem(self, id, preceding_block):
        while True:
            try:
                header, stream = self.cached_bundle
                start, end = header.get_range(id)
                try:
                    stream.seek(start)
                except:
                    stream.read(start - stream.tell())
                return ANS104DataItemHeader.fromstream(stream), stream, end - stream.tell()
            except:
                pass
            try:
                bundles_to_retry = set()
                bundles_tried = set()
                preceding_height = self.block_height(preceding_block)
                #for height in itertools.count(preceding_height + 1): #range(preceding_height + 1, max((data['api_block'] for _, data, *_ in self.tail))) if hasattr(self, 'tail') else itertools.count(preceding_height + 1):
                current_height = self.peer.height()
                height = min(current_height - 1, preceding_height + 1)
                while True:
                    for bundle in (*self.block_bundles(height if height <= current_height else None), *bundles_to_retry):
                        if bundle in bundles_tried:
                            continue
                        try:
                            if height <= current_height:
                                stream = self.peer.stream(bundle)
                            else:
                                assert height == current_height + 1
                                stream = self.peer.gateway_stream(bundle)
                        except ArweaveException as exc:
                            if height <= current_height:
                                logger.exception(f'peer did not provide {bundle}')
                            bundles_to_retry.add(bundle)
                            continue
                        try:
                            stream.__enter__()
                            header = ANS104BundleHeader.fromstream(stream)
                        except ArweaveNetworkException as exc:
                            stream.__exit__(None, None)
                            if exc.args[1] == 404:
                                if height <= current_height:
                                    logger.exception(f'peer did not provide chunks for {bundle}')
                                bundles_to_retry.add(bundle)
                                continue
                            raise
                        if id in header.length_by_id:
                            if height > self.tail_height:
                                self.tail_height = height
                            if self.cached_bundle is not None:
                                old_header, old_stream = self.cached_bundle
                                old_stream.__exit__(None, None)
                            self.cached_bundle = header, stream
                            start, end = header.get_range(id)
                            try:
                                stream.seek(start)
                            except:
                                stream.read(start - stream.tell())
                            return ANS104DataItemHeader.fromstream(stream), stream, end - stream.tell()
                        else:
                            # here is where a new bundle header has been parsed
                            # if height > current_height, there could be a new tip in the bundle
                            # UPDATE: this doesn't go here. it would happen when the stream should be extended.
                            #         i.e., either on user request or on new block found.
                            #         note that it is only needed when the stream is seeked or its length or tail queried.
                            # stubbed a self.poll() function for this, could call it at end of iterate()
                            #if height > current_height and type(self.follow_owner) is str:
                            #    offset = header.get_len_bytes()
                            #    for id, length in header.length_by_id.items():
                            #        stream.seek(offset)
                            #        candidate = ANS104DataItemHeader.fromstream(stream)
                            #        if candidate.owner == self.follow_owner:
                            #            # same owne rin this ditem. migrate tail if contains this tail.
                            #            # raise warning if doesn't.
                            #            # check constructor for data format parsing maybe
                            #            logger.warning('found another ditem with this owner!')
                            #            break
                            #        offset += length
                            stream.__exit__(None, None, None)
                            bundles_to_retry.discard(bundle)
                            if height > current_height:
                                assert height == current_height + 1
                                bundles_tried.add(bundle)
                            else:
                                bundles_tried.discard(bundle)
                    
                    if height <= current_height:
                        height += 1
                    current_height = self.peer.height()
                    if height > current_height + 1:
                        height = current_height - 1
                        self.recache_blocks_from(current_height)
                raise KeyError(dict(dataitem=id, preceding_block=preceding_block))
            except KeyError:
                logger.exception(f'{id} not found on chain (yet?), waiting 60 seconds to look again [better: look back and forward for other parts of stream as earlier code did]')
                time.sleep(60)
    def dataitem_json(self, id, preceding_block):
        header, stream, length = self.dataitem(id, preceding_block)
        return json.loads(stream.read(length))

for fn in sys.argv[1:]:
    with open(fn) as fh:
        stream = Stream(json.load(fh), Peer())#'http://gateway-4.arweave.net:1984'))
    for metadata, channel_name, time, header, stream, length in stream.iterate():
        time = datetime.datetime.fromtimestamp(time).isoformat()
        sys.stderr.write('channel data: ' + channel_name + ': ' + str(length) + ' @ ' + time + '\n')#repr(stream.read(length))+'\n')
        if channel_name == 'capture':
            sys.stdout.buffer.write(stream.read(length))
        #else:
