#!/usr/bin/env python3

import os, sys, threading, time
from collections import defaultdict, deque
from datetime import datetime
from subprocess import Popen, PIPE
import json
from ar import Peer, Wallet, DataItem, ArweaveNetworkException, logger
from ar.utils import create_tag
from bundlr import Node
from flat_tree import flat_tree
import watchdog.observers, watchdog.events
import zstandard as zstd

import logging
#logging.basicConfig(level=logging.DEBUG)

try:
    wallet = Wallet('identity.json')
except:
    print('Generating an identity ...')
    wallet = Wallet.generate(jwk_file='identity.json')

node = Node(timeout = 1)
def send(data, **tags):
    di = DataItem(data = data)
    di.header.tags = [
        create_tag(key, val, True)
        for key, val in tags.items()
    ]
    di.sign(wallet.rsa)
    while True:
        try:
            result = node.send_tx(di.tobytes())
            break
        except ArweaveNetworkException as exc:
            text, code, exc2, response = exc.args
            if code == 201: # already received
                return {'id': di.header.id}
            #pass
            logger.exception(text)
    return result

running = True

class Data:
    data = deque()
    lock = threading.Lock()
    @classmethod
    def append_needs_lk(cls, type, item):
        #if len(cls.data):
        #    entry = cls.data[-1].get(type)
        #    if entry is None:
        #        cls.data[-1][type] = [item]
        #    else:
        #        entry.append(item)
        #else:
        #    cls.data.append(dict(type=[item]))
        cls.data.append((type, item))
    @classmethod
    def extend_needs_lk(cls, type, items):
        #if len(cls.data):
        #    entry = cls.data[-1].setdefault(type, items)
        #    if entry is not items:
        #        entry.extend(items)
        #else:
        #    cls.data.append(dict(type=items))
        cls.data.extend(((type, item) for item in items))

class BinaryProcessStream(threading.Thread):
    def __init__(self, name, proc, *params, constant_output = False, **kwparams):
        super().__init__(*params, **kwparams)
        self.name = name
        self.proc = proc
        self.constant_output = constant_output
        self.start()
    def run(self):
        print(f'Beginning {self.name} ...')
        try:
            capture_proc = Popen(self.proc, stdout=PIPE)
        except:
            print(f'{self.name.title()} failed.')
            return
        capture = capture_proc.stdout
        raws = []
        while running:
            raw = capture.read1(100000)# if not self.constant_output else capture.read(100000)
            raws.append(raw)
            if Data.lock.acquire(blocking=False):
                Data.extend_needs_lk(self.name, raws)
                Data.lock.release()
                raws.clear()
                #print(len(Data.data), 'queued while running from', self.name)
        print(f'Finishing {self.name}ing')
        capture_proc.terminate()
        while True:
            #print(f'{self.name}: reading 1')
            raw = capture.read(100000)
            #print(f'{self.name} read: {len(raw)} len(raws)={len(raws)} proc.poll={capture_proc.poll()}')
            if len(raw) > 0:
                raws.append(raw)
            if len(raws):
                with Data.lock:
                    Data.extend_needs_lk(self.name, raws)
                    raws.clear()
                    print(f'Finishing {self.name}', len(Data.data))
                    #if len(Data.data[-1]) < 100000:
                    #    break
            elif capture_proc.poll() is not None:
                break
        print(f'Finished {self.name}')

class Locationer(threading.Thread):
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.start()
    def run(self):
        print('Locationing ...')
        raws = []
        last = None
        while running:
            try:
                location_proc = Popen('termux-location', stdout=PIPE)
            except:
                print('Locationing failed.')
                break
            raw = json.load(location_proc.stdout)
            raws.append(raw)
            if Data.lock.acquire(blocking=False):
                Data.extend_needs_lk('location', raws)
                Data.lock.release()
                raws.clear()
        print('Locationing finished')

class PathWatcher(threading.Thread, watchdog.events.FileSystemEventHandler):
    def __init__(self, path, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.path = path
        self.compressor = zstd.ZstdCompressor(write_checksum=True,write_content_size=True)
        self.chunker = None
        self.start()
        #self.run()
    def run(self):
        print(f'Watching {self.path} ...')
        self.observer = watchdog.observers.Observer()
        self.observer.schedule(self, self.path, recursive=True)
        self.cur_file = None
        self.cur_filename = ''
        self.just_closed = True
        self.queued_files = set()
        try:
            self.observer.start()
        except:
            print(f'{self.path} failed')
            return
        try:
            while running:
                time.sleep(0.1)
        finally:
            self.observer.stop()
            self.observer.join()
            self.close_file()
            self.process_queue()
            self.close_file()
    def start_file(self, event):
        assert self.cur_file is None
        if event.is_directory:
            return
        with Data.lock:
            Data.append_needs_lk(self.path, event.src_path.encode() + b'\0')
        self.just_closed = True
        try:
            self.cur_file = open(event.src_path, 'rb')
        except:
            self.cur_file = None
            return False
        self.cur_filename = event.src_path
        self.chunker = self.compressor.chunker(chunk_size=100000)
        self.continue_file(event)
    def continue_file(self, event):
        #print('continue file')
        assert event is None or self.cur_filename == event.src_path
        start = self.cur_file.tell()
        while True:
            in_chunk = self.cur_file.read()
            if not in_chunk:
                break
            for out_chunk in self.chunker.compress(in_chunk):
                with Data.lock:
                    Data.append_needs_lk(self.path, out_chunk)
        end = self.cur_file.tell() - start
        if end > start:
            self.just_closed = False
    def close_file(self):
        if self.cur_file is not None:
            self.continue_file(None)
            self.cur_file.close()
            self.cur_file = None
            print('finish')
            for out_chunk in self.chunker.finish():
                with Data.lock:
                    Data.append_needs_lk(self.path, out_chunk)
            self.chunker = None
    def on_created(self, event):
        #print('on_created', event.src_path)
        if event.is_directory:
            return
        if self.cur_file is not None:
            if self.just_closed:
                self.close_file()
            else:
                self.queued_files.add(event.src_path)
                return
        self.start_file(event)
    def on_modified(self, event):
        if event.is_directory:
            return
        #print('on_modified', event.src_path)
        if self.cur_filename is None:
            self.start_file(event)
        elif self.cur_filename == event.src_path:
            self.continue_file(event)
        elif self.just_closed:
            self.close_file()
            self.start_file(event)
        else:
            #print('just_closed is False, queueing')
            self.queued_files.add(event.src_path)
    def on_closed(self, event):
        if event.is_directory:
            return
        #print('on_closed', event.src_path)
        if self.cur_file is not None and self.cur_filename == event.src_path:
            #print('close: continue')
            self.continue_file(event)
            if len(self.queued_files):
                #print('files queued')
                self.close_file()
            else:
                #print('just_closed = true')
                self.just_closed = True
        self.process_queue()
    def process_queue(self):
        if self.cur_file is None:
            while len(self.queued_files):
                queued = self.queued_files.pop()
                if self.cur_filename != queued:
                    self.close_file()
                self.on_created(watchdog.events.FileSystemEvent(queued))
    def on_moved(self, event):
        if event.is_directory:
            return
        if self.cur_file is not None and self.cur_filename == event.src_path:
            self.cur_filename = event.dest_path
            self.continue_file(None)

class Storer(threading.Thread):
    input_lock = threading.Lock()
    lock = threading.Lock()
    idx = 0
    output_idx = 0
    proc_idx = 0
    pool = set()
    output = defaultdict(deque)
    #reader = BinaryProcessStream('capture', './capture')
    readers = [
        BinaryProcessStream('capture', ('sh','-c','./capture | tee last_capture.log.bin'), constant_output = True),
        Locationer(),
        BinaryProcessStream('logcat', 'logcat', constant_output = True),
        BinaryProcessStream('journalctl', ('journalctl', '--follow')),
        PathWatcher(os.path.abspath('.')),
        PathWatcher('/sdcard/Download'),
    ]
    exceptions = []
    logs = {}
    def __init__(self, *params, **kwparams):
        super().__init__(*params, **kwparams)
        self.node = Node()
        self.pending_input = deque()
        self.pending_output = deque()
        self.start()
    def print(self, *params):
        return
        log = self.logs[self.proc_idx]
        if not len(log) or log[-1] != params:
            self.logs[self.proc_idx].append(params)
            print(*params)
    def run(self):
        with self.input_lock:
            self.proc_idx = Storer.proc_idx
            self.logs[self.proc_idx] = []
            Storer.proc_idx += 1
            self.pool.add(self)
            self.print(self.proc_idx, 'launching storing')
        last_log_time = 0
        last_log_time_2 = 0
        while True:
            try:
                while len(self.pending_input):
                    next_idx, next_type, next_data = self.pending_input[0]
                    self.print(self.proc_idx, 'sending', next_idx)
                    if type(data) is bytes:
                        result = send(next_data)
                        #print(self.proc_idx, 'sent', idx)
                        result['length'] = len(next_data)
                    elif type(data) is dict:
                        result = dict(id = next_data)
                    else:
                        raise AssertionError(f'unexpected content datatype {type(data)}: {channel}, {data}')
                    self.pending_output.append((next_idx, next_type, result))
                    self.pending_input.popleft()
                while len(self.pending_output) and self.pending_output[0][0] == self.output_idx:
                    next_idx, next_type, next_result = self.pending_output.popleft()
                    self.print(self.proc_idx, 'taking storing lock with the next item')
                    with self.lock:
                        #print(self.proc_idx, 'took storing lock')
                        self.output[next_type].append(next_result)
                        self.print(self.proc_idx, 'stored', Storer.output_idx, 'index queue size =', len(self.output))
                        Storer.output_idx += 1
                if len(self.pending_output) and self.pending_output[0][0] != self.output_idx:
                    self.print(self.pending_output[0][0], 'not queuing, waiting for', self.output_idx)
                    #print(self.logs)
                #print(self.proc_idx, 'taking input_lock and Data.lock')
                with self.input_lock, Data.lock:
                    if time.time() > last_log_time + 1:
                        last_log_time = time.time()
                        self.print(self.proc_idx, 'took input_lock and Data.lock')
                    #with self.reader.lock:
                    #print(self.proc_idx, 'took reader_lock')
                    if len(Data.data) == 0:
                        if len(self.pending_input) or len(self.pending_output):# or (len(self.pool) == 1 and self.reader.is_alive()):
                            continue
                        if time.time() > last_log_time_2 + 1:
                            last_log_time_2 = time.time()
                            self.print('Stopping', self.proc_idx, 'len(self.pending) =', len(self.pending_input), len(self.pending_output), '; len(self.pool) =', len(self.pool), '; self.reader.is_alive() =', *(reader.is_alive() for reader in self.readers))
                        raise StopIteration()
                    channel, data = Data.data.popleft()
                    if len(Data.data) > len(self.pool) * (len(self.pending_input) + len(self.pending_output)):
                        self.print(self.proc_idx, 'spawning new; expected idx =', Storer.proc_idx)
                        Storer()
    
                    idx = Storer.idx
                    Storer.idx += 1
                self.pending_input.append((idx, channel, data))
                continue
            except StopIteration:
                self.print(self.proc_idx, 'finishing')
                pass
            except Exception as exc:
                self.print(self.proc_idx, 'raised exception', type(exc))
                with self.lock:
                    self.exceptions.append(exc)
            with Data.lock:
                if len(self.pool) == 1 and any((reader.is_alive() for reader in self.readers)) and not len(self.exceptions):
                    #print(self.proc_idx, 'closed but reader still running, continuing anyway')
                    continue
                self.pool.remove(self)
                self.print('storers remaining:', *(storer.proc_idx for storer in self.pool))
                break
Storer()

first = None
start_block = None
prev = None
peer = Peer(retries=9999999)
offset = 0
indices = flat_tree(3) #append_indices(3)
prev_indices_snap = indices.snap()
#index_values = indices

current_block = peer.current_block()
last_time = time.time()

prev = None

data = None
while True:
    try:
        #print('taking Storer lock')
        if data is None:
            with Storer.lock:
                if len(Storer.exceptions):
                    for exception in Storer.exceptions:
                        raise exception
                data = Storer.output.copy()
                Storer.output.clear()
                if not len(data):
                    if not running and not len(Storer.pool) and not any((reader.is_alive() for reader in Storer.readers)):
                        print('index thread stopping no output left')
                        break
                    try:
                        Storer.lock.release()
                        #print('no output to index')
                        time.sleep(0.1)
                    finally:
                        Storer.lock.acquire()
                    data = None
                    continue
                print('indexing', len(data.get('capture', [])), 'captures, releasing Storer lock')
            if time.time() > last_time + 60:
                current_block = peer.current_block()
                last_time = time.time()
        else:
            pass
            #print('data left over')
        # this could be a dict of lengths
        lengths = sum((capture['length'] for capture in data.get('capture', [])))
        datas = {
            type: dict(
                ditem = [item['id'] for item in items],
                length = sum((item['length'] for item in items))
            )
            for type, items in data.items()
        }
        indices.append(
            prev,
            lengths,
            dict(
                **datas,
                min_block = (current_block['height'], current_block['indep_hash']),
                api_block = max((0, *(item['block'] for items in data.values() for item in items if 'block' in item))) or None,
            )
        )
        indices_snap = indices.snap()
        try:
            result = send(json.dumps(indices_snap).encode())
        except:
            indices = flat_tree(3, prev_indices_snap)
            raise
        prev_indices_snap = indices_snap
        prev = dict(
            ditem = [result['id']],
            min_block = (current_block['height'], current_block['indep_hash']),
            api_block = result['block']
        )
        data = None
        if first is None:
            first = result['id']
            start_block = current_block['indep_hash']
    
        #eta = current_block['timestamp'] + (result['block'] - current_block['height']) * 60 * 2
        #eta = datetime.fromtimestamp(eta)
        #index_values = [value for leaf_count, value in indices]
        with open(first, 'wt') as fh:
            #json.dump(index_values[-1], fh)
            json.dump(prev, fh)
        #json.dump(index_values[-1], sys.stdout)
        json.dump(prev, sys.stdout)
        sys.stdout.write('\n')# + str(type(data)) + '\n')
    except KeyboardInterrupt:
        print('got a keyboard interrupt, setting running to false')
        if not running:
            break
        running = False
