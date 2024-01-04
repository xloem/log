#!/usr/bin/env python3
import socket
import requests
import os, shutil
import sys
import tqdm
import av
import json
import math
import fractions, time, logging, concurrent.futures, ar, hashlib, ar.utils, bundlr, flat_tree, nonblocking_stream_queue
import _hashlib

def path_to_pre_time(path):
    _, name = path.rsplit('/',1)
    if '.' in name:
        name, _ = name.split('.',1)
    assert len(name) == 17 and name[10] == '_'
    secs = name[:10]
    micros = name[11:]
    assert secs.isdigit() and micros.isdigit()
    return fractions.Fraction(secs+'.'+micros)

def mp4_to_raws(path):
    start_time = path_to_pre_time(path)
    with open(path,'rb') as f:
        try:
            c = av.open(f)
        except av.error.InvalidDataError as e:
            f.seek(0)
            yield [start_time, f.read(), start_time] # this should be what was yielded
            return
        with c as c:
            offset = 0 ### using offset instead of packet.pos outputs the header with the first packet, if wanted
            for packet in c.demux():
                #import pdb; pdb.set_trace()
                if packet.dts is None: # flush
                    assert not packet.size
                    continue
                start = start_time + packet.pts * packet.time_base # pts is real time; the frames can be out of order. dts is algorithmic order for decoding, when frames depend on future data.
                end = start_time + (packet.pts + packet.duration) * packet.time_base
                assert packet.size == packet.buffer_size
                tail = packet.pos + packet.size
                assert tail > offset
                assert packet.pos == offset or offset == 0
                #assert tail == f.tell()
                stash = f.tell()
                #if stash != tail:
                #    print('fp at', stash, 'but packet ends at', tail)
                
                f.seek(offset)
                raw = f.read(tail - offset)
                offset = tail
                f.seek(stash)
                #print(start, len(raw), packet, end)
                yield [start, raw, end]
            f.seek(0,2)
            assert offset == f.tell()

def free_space_bar(name, used, available):#, **kwparams):
    pbar = tqdm.tqdm(
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}{postfix}',
        total=available+used,
        desc=f'{name} usage',
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    )
    update = pbar.update
    update(used)
    pbar.refresh()
    def update_(new_used, new_available):
        nonlocal used, available
        if new_used is not None:
            used += new_used
        if new_available is not None:
            available = new_available
        pbar.total = used + available
        update(new_used)
    pbar.update = update_
    return pbar

def stream_size_hash(stream, *hashes):
    hashes = [hash() for hash in hashes]
    size = 0
    while True:
        chunk = stream.read(1024*1024)
        if not len(chunk):
            break
        size += len(chunk)
        for hash in hashes:
            hash.update(chunk)
    return [size, *[hash.hexdigest() for hash in hashes]]

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, fractions.Fraction):
            digits = math.ceil(math.log(obj.denominator, 10))
            mul = 10 ** digits
            obj *= mul
            assert obj.denominator == 1
            fixed = str(obj)
            return fixed[:-digits] + '.' + fixed[-digits:]
        else:
            return super().default(self, obj)

class Local:
    def __init__(self):
        self.path = os.path.basename(__file__).rsplit('.',1)[0]+'_stash'
        os.makedirs(self.path, exist_ok=True)
    def get(self, fn):
        fn = fn.rsplit('/',1)[-1]
        return self.File(self, fn)
    def send(self, fn, stream):
        fn = fn.rsplit('/',1)[-1]
        with open(os.path.join(self.path, fn), 'wb') as f:
            for chunk in stream:
                f.write(chunk)
    def confirm(self, fn, confirmation):
        with open(os.path.join(self.path, fn), 'w') as f:
            json.dump(confirmation, f)
    class File:
        def __init__(self, local, fn):
            self._local = local
            self._fn = fn
            self.path = os.path.join(local.path, fn)
        @property
        def exists(self):
            return os.path.exists(self.path)
        #def recv(self, chunk=1024*1024):
        #    with open(self.path, 'rb') as f:
        #        while True:
        #            data = f.read(chunk)
        #            if not data:
        #                break
        #            yield data
        def recv_dict(self):
            with open(self.path, 'r') as f:
                return json.load(f)
        def recv_raws(self):
            return mp4_to_raws(self.path)
        def rm(self):
            os.unlink(self.path)
        @property
        def size(self):
            return os.path.getsize(self.path)
        def locator(self):
            return type(self)(self._local, self._fn + '.locator')
    @property
    def available(self):
        total, used, free = shutil.disk_usage(self.path)
        return free
    def videos(self):
        return [self.File(self, subpath) for subpath in os.listdir(self.path)]

class HDC:
    def __init__(self):
        self.host = '192.168.0.10'
        self.protohost = 'http://'+self.host+':'
        self.videopath = '/mnt/usb/recording/video/'
        self.session = requests.Session()
        self._req('GET', 5000, 'ping', timeout=1)
    def cmd(self, cmd):
        #print('>', cmd)
        try:
            resp = self._req('POST', 5000, 'api/1/cmd', json=dict(cmd=cmd)).json()
        except ConnectionError as e:
            import pdb; pdb.set_trace()
            raise e
        resp = resp.get('output','') + resp.get('error','')
        #print('<', repr(resp))
        return resp
    #@property
    #def connected(self):
    #    try:
    #        self._req('GET', 5000, 'ping', timeout=1)
    #        return True
    #    except requests.ConnectionError:
    #        return False
    def _req(self, method, port, path='', **kwparams):
        try:
            return self.session.request(method, self.protohost + str(port) + '/' + path, **kwparams)
        except requests.ConnectionError as er:
            raise

    class File:
        def __init__(self, hdc, path, size=None):
            self.hdc = hdc
            self.path = path
            if size is not None:
                self.size = size
            assert "'" not in path
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
            EOF = 'EOF'
            while EOF in str:
                EOF = base64.b64encode(random.randbytes(16))
            assert self.hdc.cmd(f"cat <<{EOF} | head -c -1 >'{self.path}'; echo $?\n{str}\n{EOF}").rstrip() == '0'
        def b2sum(self):
            #self.size # checks size is same
            return self.call('b2sum').rsplit(' ',1)[0]#, cache=True).rsplit(' ',1)[0]
        @property
        def size(self):
            return self.call('stat -c %s', type=int)#, cache=True, check=True)
        @property
        def subfiles(self):
            return [
                #type(self)(self.hdc, self.path.rstrip('/')+'/'+subfile, self.httpport, self.httppath+'/'+subfile, root=self)
                type(self)(self.hdc, self.path.rstrip('/')+'/'+subfile)
                for subfile in self.call('ls').strip().split('\n')
            ]
        def recv_prep(self):
            self.prepped = True
            self.port = (int.from_bytes(hashlib.sha256(self.path.encode()).digest(),'little') % 32768) + 32768
        def recv(self, chunk=1024*1024):
            assert self.prepped
            self.prepped = False
            self.hdc.cmd(f"nc -l -p {self.port} < '{self.path}' > /dev/null 2>&1 &")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            sock.connect((self.hdc.host, self.port))
            with tqdm.tqdm(desc=self.path.rsplit('/',1)[1],total=self.size,unit='B',unit_divisor=1024,unit_scale=True,leave=False) as pbar:
                while True:
                    data = sock.recv(chunk)
                    if not data:
                        break
                    yield data
                    pbar.update(len(data))
    @property
    def available(self):
        header, avail = self.cmd(f"df --output=avail {self.videopath}").rstrip().split('\n')
        assert header.lstrip() == 'Avail'
        return int(avail) * 1024
    def videos(self):
        return self.File(self, self.videopath).subfiles

class ArDItemLengths:
    def __init__(self):#, **tags):
        try:
            self.wallet = ar.Wallet('identity.json')
        except:
            print('.. identity.json ..')
            self.wallet = ar.Wallet.generate(jwk_file='identity.json')
        #self.bundlrstorage = self.BundlrStorage(**tags)
    #@property
    #def connected(self):
    #    return self.bundlrstorage.connected
    #def send(self, fn, stream):
    #    
    def send_raws(self, raws_iter, **tags):
        hash_algs = {
            name: val
            for name, val in tags.items()
            if isinstance(val, _hashlib.HASH) or isinstance(val, hashlib.blake2b) or isinstance(val, hashlib.blake2s)
        }

        bundlrstorage = self.BundlrStorage(self.wallet)
        indices = flat_tree.flat_tree(bundlrstorage, 3)
        raws = []
        for pre_time, next_buf, post_time in raws_iter:
            if len(next_buf) <= 100000:
                raws.append([pre_time, next_buf, post_time])
            else:
                raws.extend([
                    [pre_time, next_buf[off:off+100000], post_time]
                    for off in range(0, len(next_buf), 100000)
                ])
            #while len(buf) < 100000*64:
            #    try:
            #        buf += next(stream)
            #    except:
            #        break
            #if not buf:
            #    break
                    # the chunks are greater than 100000 I made a mistake.
                        # the timestamps would be roughly the same i suppose
            #raws = [buf[idx*100000:(idx+1)*100000] for idx in range(min(64,len(buf)//100000))]
            #buf = buf[100000*64:]
            #time = path_to_pre_time(fn)

            while len(raws):
                new_idx = bundlrstorage.store_data(raws[:64])
                for hash in hash_algs.values():
                    for _, raw, _ in raws[:64]:
                        hash.update(raw)
                bundlrstorage.tags = {
                    **tags,
                    **{ name: alg.digest() for name, alg in hash_algs.items() }
                }
                indices.append(
                    sum([len(raw) for _, raw, _ in raws[:64]]),
                    new_idx
                )
                bundlrstorage.tags = {}
                raws = raws[64:]
                #prev_indices_id = indices.locator
                #if first is None:
                #    first = prev_indices_id['ditem'][0]
                #    start_block = self.bundlrstorage.current_block['indep_hash']
        return json.dumps(indices.locator, cls=JSONEncoder) #prev_indices_id)

    class BundlrStorage:
        def __init__(self, wallet=PermissionError('no wallet provided'), **tags):
            self.peer = ar.Peer(ar.PUBLIC_GATEWAYS[1], timeout=1)
            self.node = bundlr.Node(timeout=1)
            self.tags = tags
            self._current_block = self.peer.current_block()
            self._last_block_time = time.time()
            self.node.info()
            self.wallet = wallet
        #@property
        #def connected(self):
        #    try:
        #        self.peer.info()
        #        self.node.info()
        #    except requests.ConnectionError:
        #        return False
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
            data = json.dumps(metadata, cls=JSONEncoder).encode()
            result = self.send(data)
            confirmation = self.send(json.dumps(result, cls=JSONEncoder).encode())
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
            #global dropped_ct, dropped_size # for quick implementation
            #data_array = []
            #for offset in range(0,len(raw),100000):
            #    data_array.append(send(raw[offset:offset+100000]))
            #data_array = [self.send(raw) for pre_time, raw, post_time in raws]
            data_array = list(concurrent.futures.ThreadPoolExecutor(max_workers=4).map(self.send, [raw for pre_time, raw, post_time in raws]))
            confirmation = self.send(json.dumps(data_array, cls=JSONEncoder).encode())
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
                #dropped = dict(
                #    count = dropped_ct,
                #    size = dropped_size,
                #    time = last_post_time,
                #) if dropped_ct else None,
            )
        def send(self, data, **tags):
            di = ar.DataItem(data = data)
            di.header.tags = [
                ar.utils.create_tag(key, val, True)
                for key, val in {**self.tags, **tags}.items()
            ]
            di.sign(self.wallet.rsa)
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
    print('Observing ...')

    local = Local()
    local_size = sum([video.size for video in local.videos()])
    with free_space_bar('Local', local_size, local.available) as local_usage:
        hdc_connected = False
        remote_connected = False
        try:
            hdc = HDC()
            hdc_connected = True
            #hdc_size = sum([video.size for video in hdc.videos()])
            hdc_usage = free_space_bar('Device', 0, hdc.available)
        except requests.ConnectionError as e:
            hdc = e
        if not hdc_connected:
            try:
                remote = ArDItemLengths()
                remote_connected = True
            except ar.ArweaveNetworkException as e:
                remote = e

        if remote_connected:
            for v in tqdm.tqdm(local.videos(), desc='Sending', unit='m'):
                import pdb; pdb.set_trace()
                locator_path = v.path + '.locator'
                if not os.path.exists(locator_path):
                    locator = remote.send_raws(v.recv_raws(), fn=v.path.rsplit('/',1)[-1], sha256 = hashlib.sha256(), blake2b = hashlib.blake2b())
                    with open(locator_path, 'w') as f:
                        f.write(locator)
                else:
                    with open(locator_path, 'r') as f:
                        locator = f.read()
                locator = json.loads(locator)
                subproc = subprocess.run(['python3', 'download.py', locator_path], stdout=subprocess.PIPE)
                import pdb; pdb.set_trace()
                with open(v.path, 'rb') as f:
                    size, sha256, blake2b = stream_size_hash(f, hashlib.sha256, hashlib.blake2)
                size_2, sha256_2, blake2b_2 = stream_size_hash(subproc.stdout, hashlib.sha256, hashlib.blake2)
                assert size == size2 and sha256 == sha256_2 and blake2b == blake2b_2
                #    
               # 
               # if size == v.size and sha256 == v.sha256 and 
               # local.File(local, v
        elif hdc_connected:
            with hdc_usage:
                videos = []
                for v in tqdm.tqdm(hdc.videos(), desc='Prepping', leave=False, unit='m'):
                    size = v.size
                    hdc_usage.update(size, None)
                    local_v = local.get(v.path)
                    if not local_v.exists or size != local_v.size: # or v.b2sum != local_v.b2sum:
                        videos.append([v, size])
                        v.recv_prep()
                printed_notice = False
                with tqdm.tqdm(videos, desc='Retrieving', unit='m') as pbar:
                    for video, video_size in pbar:
                        #if video_size + local_size > (local.available + local_size) // 2 and not printed_notice:
                        #    local_usage.desc = 'Local usage exceeds local free !!'
                        #    #pbar.display('local usage exceeds local free; free some space or upload')
                        #    #print()
                        #    #pbar.update()
                        #    #printed_notice = True
                        if video_size > local.available:
                            #print()
                            pbar.display('ran out of disk space locally')
                            break
                        local.send(video.path, video.recv())
                        local_usage.update(video_size, local.available)
                    else:
                        pbar.display('Done.')
                    pbar.refresh()
        else:
            print('Failed to connect to anything.')

        local_size = sum([video.size for video in local.videos()])
        local_usage.update(local_size, local.available)
if __name__ == '__main__':
    main()
