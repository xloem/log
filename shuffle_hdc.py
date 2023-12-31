#!/usr/bin/env python3

import socket
import requests
import os
import sys
import tqdm
import decimal, time, logging, concurrent.futures, ar, hashlib, ar.utils, bundlr, flat_tree, nonblocking_stream_queue

def path_to_pre_time(path):
    _, name = path.rsplit('/',1)
    if '.' in name:
        name, _ = name.split('.',1)
    assert len(name) == 17 and name[10] == '_'
    secs = name[:10]
    micros = name[11:]
    assert secs.isdigit() and micros.isdigit()
    return decimal.Decimal(secs+'.'+micros)

class Local:
    def __init__(self):
        self.path = 'stash'
    def send(self, fn, stream):
        with open(os.path.join(self.path, fn), 'wb') as f:
            for chunk in stream:
                f.write(chunk)
    class File:
        def __init__(self, local, fn):
            self.path = os.path.join(local.path, fn)
        def recv(self, chunk=1024*1024):
            with open(self.path, 'rb') as f:
                while True:
                    data = f.read(chunk)
                    if not data:
                        break
                    yield data
        def rm(self):
            os.unlink(self.path)
    def videos(self):
        return [self.File(os.path.join(self.path, subpath) for subpath in os.readdir(self.path)]

class HDC:
    def __init__(self):
        self.host = '192.168.0.10'
        self.protohost = 'http://'+self.host+':'
        self.videopath = '/mnt/usb/recording/video/'
        self.session = requests.Session()
    def cmd(self, cmd):
        #print('>', cmd)
        resp = self._req('POST', 5000, 'api/1/cmd', json=dict(cmd=cmd)).json()
        resp = resp.get('output','') + resp.get('error','')
        #print('<', repr(resp))
        return resp
    @property
    def connected(self):
        try:
            self._req('GET', 5000, 'ping', timeout=1)
            return True
        except requests.ConnectionError:
            return False
    def _req(self, method, port, path='', **kwparams):
        try:
            return self.session.request(method, self.protohost + str(port) + '/' + path, **kwparams)
        except requests.ConnectionError as er:
            raise

    class File:
        def __init__(self, hdc, path):
            self.hdc = hdc
            self.path = path
        @property
        def pre_time(self):
            #_, name = self.path.rsplit('/',1)
            #if '.' in name:
            #    name, _ = name.split('.',1)
            #if len(name) == 17 and name[10] == '_':
            #    secs = name[:10]
            #    micros = name[11:]
            #    if secs.isdigit() and micros.isdigit():
            #        return decimal.Decimal(secs+'.'+micros)
            return path_to_pre_time(self.path)
            assert not 'filename was not sec micro, could use stat -c %W or %X'
        @property
        def post_time(self):
            return self.call('stat -c %Y', type=int)
        def cachefile(self, fn):
            dir, name = self.path.rsplit('/',1)
            return type(self)(self.hdc,f'{dir}/.{name}.cache.{fn}')
        def call(self, call, cache=False, check=False, type=None, status=False):
            if cache:
                cache = self.cachefile(call.replace(' ','_'))
                if cache.exists:
                    cacheresult = cache.cat()
            if not cache or not cache.exists or check:
                if status:
                    if status == 'keepoutput':
                        cmdresult = self.hdc.cmd(f"{call} '{self.path}'; echo $?")
                    else:
                        cmdresult = self.hdc.cmd(f"{call} '{self.path}' > '/dev/null'; echo $?")
                else:
                    cmdresult = self.hdc.cmd(f"{call} '{self.path}'")
                if check and cache and cache.exists:
                    if cmdresult != cacheresult:
                        print('outdated:', call, cacheresult)
                        assert self.hdc.cmd(f"rm '{self.cachefile('').path}'*; echo $?").strip() == '0'
                if cache and not cache.exists:
                    cache.set(cmdresult)
                result = cmdresult
            else:
                result = cacheresult
            if type is None:
                type = int if status else str
            return type(result)
        @property
        def exists(self):
            return self.call('test -e', status=True) == 0
        def cat(self):
            return self.call('cat')
        def rm(self):
            assert self.call('rm', status=True) == 0
        def set(self, str):
            escaped = str.replace("'", "'\"'\"'")
            assert self.call(f"echo -n '{escaped}' >", status='keepoutput') == 0
        def b2sum(self):
            self.size # checks size is same
            return self.call('b2sum', cache=True).rsplit(' ',1)[0]
        @property
        def size(self):
            return self.call('stat -c %s', type=int, cache=True, check=True)
        @property
        def subfiles(self):
            return [
                #type(self)(self.hdc, self.path.rstrip('/')+'/'+subfile, self.httpport, self.httppath+'/'+subfile, root=self)
                type(self)(self.hdc, self.path.rstrip('/')+'/'+subfile)
                for subfile in self.call('ls').split('\n')
            ]
        def recv(self, chunk=1024*1024):
            port = 7000
            self.hdc.cmd(f"nc -l -p {7000} < '{self.path}' > /dev/null 2>&1 &")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            sock.connect((self.hdc.host, port))
            with tqdm.tqdm(desc=self.path.rsplit('/',1)[1],total=self.size,unit='B',unit_divisor=1024,unit_scale=True) as pbar:
                while True:
                    data = sock.recv(chunk)
                    if not data:
                        break
                    yield data
                    pbar.update(len(data))

        #def http_iter_content(self):
        #    httppath = self.httppath.lstrip('/')
        #    try:
        #        if self.root.httpport is None:
        #            self.root.httpport = (int.from_bytes(self.root.path.encode(),'little') % (65536-1024))+1024
        #            raise requests.ConnectionError()
        #        resp = self.hdc._req('GET', self.root.httpport, httppath, stream=True, timeout=1)
        #    except requests.ConnectionError:
        #        print('LAUNCHING', self.root.path, self.root.httpport)
        #        output=self.hdc.cmd(f"nohup python3 -m http.server -d '{self.root.path}' {int(self.root.httpport)} >/dev/null 2>&1 &")
        #        resp = self.hdc._req('GET', self.root.httpport, httppath, stream=True, timeout=2)
        #    with tqdm.tqdm(desc=httppath,total=self.size,unit='B',unit_divisor=1024,unit_scale=True) as pbar:
        #        for chunk in resp.iter_content(1024*1024):
        #            yield chunk
        #            pbar.update(len(chunk))

    def videos(self):
        return self.File(self, self.videopath).subfiles

class ArDItemLengths:
    def __init__(self, **tags):
        try:
            self.wallet = ar.Wallet('identity.json')
        except:
            print('.. identity.json ..')
            self.wallet = ar.Wallet.generate(jwk_file='identity.json')
        self.bundlrstorage = self.BundlrStorage(**tags)
    def send(self, fn, stream):
        indices = flat_tree.flat_tree(self.bundlrstorage, 3)
        buf = b''
        while True:
            while len(buf) < 100000*64:
                try:
                    buf += next(stream)
                except:
                    break
            if not buf:
                break
            raws = [buf[idx*100000:(idx+1)*100000] for idx in range(min(64,len(buf)//100000))]
            buf = buf[100000*64:]
            time = path_to_pre_time(fn)
            indices.append(
                sum([len(raw) for raw in raws]),
                self.bundlrstorage.store_data([[time, raw, None] for raw in raws])
            )
            prev_indices_id= indices.locator
            last_raw = raws[-1]
            if first is None:
                first = prev_indices_id['ditem'][0]
                start_block = self.bundlrstorage.current_block['indep_hash']
        return json.dumps(prev_indices_id)

    class BundlrStorage:
        def __init__(self, **tags):
            self.peer = ar.Peer()
            self.node = bundlr.Node()
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
            data = json.dumps(metadata).encode()
            result = self.send(data)
            confirmation = self.send(json.dumps(result).encode())
            sha256 = hashlib.sha256()
            sha256.update(data)
            sha256 = sha256.hexdigest()
            blake2b = hashlib.blake2b()
            blake2b.update(data)
            blake2b = blake2b.hexdigest()
            return dict(
                ditem = [result['id']],
                min_block = (self.current_block['height'], self.current_block['indep_hash']),
                #api_block = result['block'],
                rcpt = confirmation['id'],
                sha256 = sha256,
                blake2b = blake2b,
            )
        def store_data(self, raws):
            global last_post_time # so it can start prior to imports
            global dropped_ct, dropped_size # for quick implementation
            #data_array = []
            #for offset in range(0,len(raw),100000):
            #    data_array.append(send(raw[offset:offset+100000]))
            #data_array = [self.send(raw) for pre_time, raw, post_time in raws]
            data_array = list(concurrent.futures.ThreadPoolExecutor(max_workers=4).map(self.send, [raw for pre_time, raw, post_time in raws]))
            confirmation = self.send(json.dumps(data_array).encode())
            sha256 = hashlib.sha256()
            for pre, raw, post in raws:
              sha256.update(raw)
            sha256 = sha256.hexdigest()
            blake2b = hashlib.blake2b()
            for pre, raw, post in raws:
              blake2b.update(raw)
            blake2b = blake2b.hexdigest()
            return dict(
                capture = dict(
                    ditem = [data['id'] for data in data_array],
                    time = [pre_time for pre_time, raw, post_time in raws],
                ),
                min_block = (self.current_block['height'], self.current_block['indep_hash']),
                #api_block = data_array[-1]['block'],
                rcpt = confirmation['id'],
                sha256 = sha256,
                blake2b = blake2b,
                dropped = dict(
                    count = dropped_ct,
                    size = dropped_size,
                    time = last_post_time,
                ) if dropped_ct else None,
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

def main():
    hdc = HDC()
    assert hdc.connected
    videos = hdc.videos()
    for video in videos:
        print(video.path)
        print(video.size)
        print(video.b2sum())
        print(sum([len(chunk) for chunk in video.recv()]))
main()
