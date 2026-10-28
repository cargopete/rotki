"""A nuthatch nest run inside rotki as a WebAssembly module, for users with no nest of their own.

The module is nuthatch's address history (txlist, txlistinternal, the token transfer lists and
getblocknobytime) compiled to wasm32. It opens no file and no socket: every JSON-RPC call it needs
is handed to rotki, which sends it to the user's own Ethereum node, and its store is a byte buffer
rotki keeps in the encrypted user database. It answers the same Etherscan-shaped requests a nest
answers at /api, so the Nuthatch indexer reaches it through a requests transport adapter and
parses its answers exactly as it parses a nest's.
"""
import base64
import hashlib
import json
import logging
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import parse_qsl, urlsplit

import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from rotkehlchen.db.cache import DBCacheStatic
from rotkehlchen.errors.misc import RemoteError
from rotkehlchen.logging import RotkehlchenLogsAdapter
from rotkehlchen.types import ChainID, SupportedBlockchain

if TYPE_CHECKING:
    from requests import PreparedRequest

    from rotkehlchen.db.dbhandler import DBHandler

logger = logging.getLogger(__name__)
log = RotkehlchenLogsAdapter(logger)

# requests prepares query strings only for http URLs. `.invalid` never resolves, so nothing could
# leave the machine even if the adapter were not mounted.
EMBEDDED_NEST_URL: Final = 'http://nuthatch-embedded.invalid'
# Built from nuthatch-org/nuthatch at the commit below by `embed/build-wasm.sh`, which gives the
# same bytes on any machine with Rust 1.95.0.
NUTHATCH_WASM_PATH: Final = Path(__file__).parent.parent / 'data' / 'nuthatch_embed.wasm'
NUTHATCH_WASM_SOURCE: Final = 'nuthatch-org/nuthatch@30719d92'
NUTHATCH_WASM_SHA256: Final = '8f4b97d48fc140da6802aadea354ab7c23e21b3739898f5f069a0181a39e9624'
# How many calls one round of the module's requests may have in flight, as a nest has natively.
RPC_CONCURRENCY: Final = 8
RPC_ATTEMPTS: Final = 4
RPC_TIMEOUT: Final = 120


class EmbeddedNest:
    """The module, loaded on first use with the store this user's database holds.

    One call at a time: a wasm instance is not shared between threads.
    """

    def __init__(self, database: DBHandler) -> None:
        self.db = database
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.pool = ThreadPoolExecutor(max_workers=RPC_CONCURRENCY, thread_name_prefix='nuthatch-rpc')  # noqa: E501
        self._instance: Any = None
        self._reply = b''
        self._endpoint = ''
        self._dirty = False

    def _node_endpoint(self) -> str:
        """The user's own Ethereum node, which must serve trace_filter and eth_getLogs over
        archive history; rotki's public nodes serve neither.

        May raise:
        - RemoteError if no owned Ethereum node is active
        """
        for node in self.db.get_rpc_nodes(blockchain=SupportedBlockchain.ETHEREUM, only_active=True):  # noqa: E501
            if node.node_info.owned and node.node_info.endpoint != '':
                return node.node_info.endpoint

        raise RemoteError('The embedded nuthatch indexer needs an owned, active Ethereum node that serves traces')  # noqa: E501

    def _load(self) -> Any:
        """May raise:
        - RemoteError if the module or wasmtime is missing, the module is not the pinned one, or
        no owned Ethereum node is configured
        """
        if self._instance is not None:
            return self._instance

        try:
            import wasmtime  # only loaded by users who turned the module on
        except ImportError as e:
            raise RemoteError(f'The embedded nuthatch indexer needs wasmtime: {e!s}') from e

        self._endpoint = self._node_endpoint()
        try:
            wasm = NUTHATCH_WASM_PATH.read_bytes()
        except OSError as e:
            raise RemoteError(f'The embedded nuthatch module could not be read: {e!s}') from e
        if (digest := hashlib.sha256(wasm).hexdigest()) != NUTHATCH_WASM_SHA256:
            raise RemoteError(f'The embedded nuthatch module is {digest}, not the pinned {NUTHATCH_WASM_SHA256}')  # noqa: E501

        engine = wasmtime.Engine()
        store = wasmtime.Store(engine)
        linker = wasmtime.Linker(engine)
        i32 = wasmtime.ValType.i32()
        linker.define_func('nuthatch', 'rpc_batch', wasmtime.FuncType([i32, i32], [i32]), self._rpc_batch, access_caller=True)  # noqa: E501
        linker.define_func('nuthatch', 'rpc_take', wasmtime.FuncType([i32], []), self._rpc_take, access_caller=True)  # noqa: E501
        instance = linker.instantiate(store, wasmtime.Module(engine, wasm))
        self._instance = (store, instance.exports(store))
        with self.db.conn.read_ctx() as cursor:
            saved = self.db.get_static_cache(cursor=cursor, name=DBCacheStatic.NUTHATCH_EMBEDDED_STATE)  # noqa: E501
        state = zlib.decompress(base64.b64decode(saved)) if saved is not None else b''
        config = json.dumps({'chain_id': ChainID.ETHEREUM.value}).encode()
        self._call('nh_open', config, state)
        log.debug('Loaded the embedded nuthatch module from %s with a %d byte store', NUTHATCH_WASM_SOURCE, len(state))  # noqa: E501
        return self._instance

    def _one(self, request: dict[str, Any]) -> dict[str, str]:
        """One JSON-RPC call the module asked for. Its JSON-RPC errors go back to it whole, since
        it narrows a ranged call on the provider's own words."""
        body = {'jsonrpc': '2.0', 'id': 1, 'method': request['method'], 'params': request['params']}  # noqa: E501
        error = ''
        for attempt in range(RPC_ATTEMPTS):
            if attempt != 0:
                time.sleep(2 ** attempt)
            try:
                response = self.session.post(self._endpoint, json=body, timeout=RPC_TIMEOUT)
            except requests.exceptions.RequestException as e:
                error = f'transport: {type(e).__name__}'
                continue
            if response.status_code == HTTPStatus.OK or '"error"' in response.text:
                return {'body': response.text}
            error = f'HTTP {response.status_code}'

        return {'error': f'no answer after {RPC_ATTEMPTS} attempts: {error}'}

    def _rpc_batch(self, caller: Any, ptr: int, length: int) -> int:
        memory = caller['memory']
        requests_ = json.loads(bytes(memory.read(caller, ptr, ptr + length)))
        self._dirty = True
        self._reply = json.dumps(list(self.pool.map(self._one, requests_))).encode()
        return len(self._reply)

    def _rpc_take(self, caller: Any, ptr: int) -> None:
        caller['memory'].write(caller, self._reply, ptr)
        self._reply = b''

    def _call(self, name: str, *blobs: bytes) -> bytes:
        """Call one of the module's exports with its inputs, and read its output.

        May raise:
        - RemoteError if the export answered an error
        """
        store, exports = self._instance
        memory, args, held = exports['memory'], [], []
        for blob in blobs:
            if len(blob) == 0:
                args += [0, 0]
                continue
            ptr = exports['nh_alloc'](store, len(blob))
            memory.write(store, blob, ptr)
            args += [ptr, len(blob)]
            held.append((ptr, len(blob)))
        result = exports[name](store, *args)
        for ptr, size in held:
            exports['nh_free'](store, ptr, size)
        output = b''
        if (size := abs(result)) != 0:
            ptr = exports['nh_alloc'](store, size)
            exports['nh_output'](store, ptr)
            output = bytes(memory.read(store, ptr, ptr + size))
            exports['nh_free'](store, ptr, size)
        if result < 0:
            raise RemoteError(f'The embedded nuthatch module failed {name}: {output.decode()}')
        return output

    def _save(self) -> None:
        """Keep the module's store in the user database, if a call may have changed it."""
        if not self._dirty:
            return
        state = self._call('nh_export')
        with self.db.user_write() as write_cursor:
            self.db.set_static_cache(
                write_cursor=write_cursor,
                name=DBCacheStatic.NUTHATCH_EMBEDDED_STATE,
                value=base64.b64encode(zlib.compress(state)).decode(),
            )
        self._dirty = False

    def query(self, params: dict[str, str]) -> dict[str, Any]:
        """An Etherscan-shaped request, answered as a nest's /api answers it.

        May raise:
        - RemoteError if the module cannot be loaded or failed
        """
        with self.lock:
            self._load()
            answer = json.loads(self._call('nh_query', json.dumps(params).encode()))
            self._save()
        return answer

    def watch(self, add: list[str], remove: list[str]) -> list[str]:
        """May raise:
        - RemoteError if the module cannot be loaded or failed
        """
        with self.lock:
            self._load()
            watched = json.loads(self._call('nh_watch', json.dumps({'add': add, 'remove': remove}).encode()))['watched']  # noqa: E501
            self._dirty = True
            self._save()
        return watched


class EmbeddedNestAdapter(BaseAdapter):
    """Serves the Nuthatch indexer's requests to `EMBEDDED_NEST_URL` from the module: /api, and
    the /ready and /api/watch calls its watch sync makes."""

    def __init__(self, nest: EmbeddedNest) -> None:
        super().__init__()
        self.nest = nest

    def send(self, request: PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        url = urlsplit(request.url or '')
        try:
            if url.path == '/api':
                body = self.nest.query(dict(parse_qsl(url.query)))
            elif url.path == '/ready':
                body = {'chain_id': ChainID.ETHEREUM.value, 'mode': 'address_history', 'embedded': True}  # noqa: E501
            elif url.path == '/api/watch' and request.method == 'GET':
                body = {'watched': self.nest.watch(add=[], remove=[]), 'validators': []}
            elif url.path in {'/api/watch', '/api/unwatch'} and request.method == 'POST':
                address = json.loads(request.body or '{}').get('address')
                if address is None:
                    return self._response(request, HTTPStatus.BAD_REQUEST, {'error': 'the embedded module watches addresses only'})  # noqa: E501
                added = url.path == '/api/watch'
                body = {'watched': self.nest.watch(add=[address] if added else [], remove=[] if added else [address])}  # noqa: E501
            else:
                return self._response(request, HTTPStatus.NOT_FOUND, {'error': f'{url.path} is not served by the embedded module'})  # noqa: E501
        except RemoteError as e:
            raise requests.exceptions.ConnectionError(str(e), request=request) from e

        return self._response(request, HTTPStatus.OK, body)

    @staticmethod
    def _response(request: PreparedRequest, status: HTTPStatus, body: dict[str, Any]) -> requests.Response:  # noqa: E501
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(body).encode()
        response.headers = CaseInsensitiveDict({'Content-Type': 'application/json'})
        response.url = request.url or ''
        response.request = request
        response.encoding = 'utf-8'
        return response

    def close(self) -> None:
        pass
