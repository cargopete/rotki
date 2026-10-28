import gzip
import json
from pathlib import Path
from typing import Any, Final
from unittest.mock import patch

import pytest

from rotkehlchen.chain.evm.types import NodeName, WeightedNode, string_to_evm_address
from rotkehlchen.chain.structures import TimestampOrBlockRange
from rotkehlchen.errors.misc import RemoteError
from rotkehlchen.externalapis.nuthatch import Nuthatch
from rotkehlchen.externalapis.nuthatch_embedded import EMBEDDED_NEST_URL, EmbeddedNest
from rotkehlchen.fval import FVal
from rotkehlchen.types import (
    ChainID,
    EvmInternalTransaction,
    EvmTransaction,
    SupportedBlockchain,
)

ADDRESS: Final = string_to_evm_address('0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045')
FROM_BLOCK: Final = 18024000
TO_BLOCK: Final = 18031500
# Every RPC answer the module needed for ADDRESS over [FROM_BLOCK, TO_BLOCK], recorded from one
# Ethereum archive node and keyed by [method, params].
REPLAY: Final = Path(__file__).parents[1] / 'data' / 'nuthatch_embedded_rpc.json.gz'
# Etherscan's hashes for ADDRESS over the same blocks.
ETHERSCAN_TXLIST: Final = {
    '0x0f84f80f8787f4e97965f30f158940723b99a2ac1231d0755627f2760b99d210',
    '0x1820b6423fce053f4735179bce5623268ecbc937b945f8d2747cee86cdd4331f',
    '0x280617074a3dba318b1d94501f9a3bc9adf223d3b7ff353c71b2e9b647248978',
    '0x2f6fd389a04d489955df71e1bb693c8953ffcd5915fc697d1a855d31e9bf06a4',
    '0x444ed40d010e721735fa328acfd9978b34648fca05b1364b41164c2ce24f40aa',
    '0x454394dec6ff7b70443657566f469a8752948f0d3eabb2e4c90bea7c41ba7385',
    '0x77e6024440dd3933f814c4a41ee1ae6c4b6d13c8fc2ca7ac56865dba9e7160a4',
    '0x8908a69a4ac0b8b798e3eb3913ca5d844c5191a5f442552abf7e62d76eb97b23',
    '0x91dd433e240c442085a2653d888b40a9c3f1f18346f705c75decfb29036c80bd',
    '0x948c98ffe16abc3b6aac457b099ab24e23cbaf9c4aadf757721d429f019cea35',
    '0xa25064822e08fecab14b8a33afe494b80147234dcb599767179f7e2fb38c3e0c',
    '0xa3b119bb85debaee4e990c189f946e3781a66d7a6cda0d82273891c15eedb0d0',
}
ETHERSCAN_TOKENTX: Final = {
    '0x036f8ab2936bef5ea2ea7e603410a55f60605f537805125b8507c249e7b36e91',
    '0x106070f6b67e069bbbc674b103440a474b92c5a627353159b6c8f57ddfe4ef06',
    '0x4a7c3837c7a84b7427c985b6eb02ef4ff96afc422f5f07bc720b6f1eeeccc935',
    '0x52d245050aa25def59c4be8ca8c7ca5deae985792507acab3e000b0bec771dd4',
    '0xae3332d458dcca808007e129d5582b42ce91d33ccaa10e2c538d892747ef63e5',
    '0xb192a1b95a455e26dc8aa3c840ea65f2da7577acee7db436fc80505ad83f7c1c',
}
ETHERSCAN_INTERNAL: Final = {'0x2a90d92a978f599e290f1c9b3b8b20426612f842f9d65fcf925ac50c745d2288'}


def _replay() -> tuple[Any, list[str]]:
    """A stand-in for the node: each request the module makes is answered from the recording,
    and one it was not recorded making is answered as a failure and remembered."""
    with gzip.open(REPLAY) as f:
        answers = json.load(f)
    unrecorded: list[str] = []

    def one(_: EmbeddedNest, request: dict[str, Any]) -> dict[str, str]:
        key = json.dumps([request['method'], request['params']], sort_keys=True)
        if (answer := answers.get(key)) is None:
            unrecorded.append(key)
            return {'error': 'not recorded'}
        return {'body': json.dumps(answer)}

    return one, unrecorded


def _own_node(database) -> None:
    database.add_rpc_node(WeightedNode(
        node_info=NodeName(
            name='own archive node',
            endpoint='http://127.0.0.1:8545',
            owned=True,
            blockchain=SupportedBlockchain.ETHEREUM,
        ),
        active=True,
        weight=FVal(1),
    ))


def _hashes(nuthatch: Nuthatch) -> tuple[set[str], set[str], set[str]]:
    period = TimestampOrBlockRange(range_type='blocks', from_value=FROM_BLOCK, to_value=TO_BLOCK)
    txs = {
        str(tx.tx_hash)
        for batch in nuthatch.get_transactions(chain_id=ChainID.ETHEREUM, account=ADDRESS, action='txlist', period_or_hash=period)  # noqa: E501
        for tx in batch if isinstance(tx, EvmTransaction)
    }
    internal = {
        str(tx.parent_tx_hash)
        for batch in nuthatch.get_transactions(chain_id=ChainID.ETHEREUM, account=ADDRESS, action='txlistinternal', period_or_hash=period)  # noqa: E501
        for tx in batch if isinstance(tx, EvmInternalTransaction)
    }
    tokens = {
        str(tx_hash)
        for batch in nuthatch.get_token_transaction_data(chain_id=ChainID.ETHEREUM, account=ADDRESS, from_block=FROM_BLOCK, to_block=TO_BLOCK)  # noqa: E501
        for tx_hash, _, _ in batch
    }
    return txs, internal, tokens


@pytest.mark.parametrize('db_settings', [{'nuthatch_embedded': True}])
def test_embedded_nest_answers_as_etherscan(database, messages_aggregator) -> None:
    """The module, given only an owned node's answers, finds the same transactions, internal
    transactions and token transfers as Etherscan, and keeps its store for the next session."""
    _own_node(database)
    nuthatch = Nuthatch(database=database, msg_aggregator=messages_aggregator)
    assert nuthatch._get_url(chain_id=ChainID.ETHEREUM) == f'{EMBEDDED_NEST_URL}/api'
    one, unrecorded = _replay()
    with patch.object(EmbeddedNest, '_one', autospec=True, side_effect=one):
        assert nuthatch.embedded.watch(add=[str(ADDRESS)], remove=[]) == [str(ADDRESS).lower()]
        assert _hashes(nuthatch) == (ETHERSCAN_TXLIST, ETHERSCAN_INTERNAL, ETHERSCAN_TOKENTX)

    assert unrecorded == []

    def offline(_: EmbeddedNest, request: dict[str, Any]) -> dict[str, str]:
        raise AssertionError(f'a covered range asked the node {request}')

    again = Nuthatch(database=database, msg_aggregator=messages_aggregator)
    with patch.object(EmbeddedNest, '_one', autospec=True, side_effect=offline):
        assert _hashes(again) == (ETHERSCAN_TXLIST, ETHERSCAN_INTERNAL, ETHERSCAN_TOKENTX)


@pytest.mark.parametrize('db_settings', [{'nuthatch_embedded': True}])
def test_embedded_nest_refuses_without_an_owned_node(database, messages_aggregator) -> None:
    """rotki's public nodes serve no traces, so with no node of the user's own the module is not
    loaded, and the refusal reaches rotki as a failure it falls through on."""
    nuthatch = Nuthatch(database=database, msg_aggregator=messages_aggregator)
    with pytest.raises(RemoteError, match='owned, active Ethereum node'):
        nuthatch._query(
            chain_id=ChainID.ETHEREUM,
            module='account',
            action='txlist',
            options={'address': str(ADDRESS), 'startblock': str(FROM_BLOCK), 'endblock': str(TO_BLOCK)},  # noqa: E501
        )


@pytest.mark.parametrize('db_settings', [{'nuthatch_embedded': True}])
def test_embedded_nest_watch_sync(database, messages_aggregator) -> None:
    """The watch sync a nest gets over HTTP reaches the module through the same calls."""
    _own_node(database)
    nuthatch = Nuthatch(database=database, msg_aggregator=messages_aggregator)
    assert nuthatch._nest_call('GET', '/ready')['chain_id'] == ChainID.ETHEREUM.value
    assert nuthatch._nest_call('POST', '/api/watch', {'address': str(ADDRESS)})['watched'] == [str(ADDRESS).lower()]  # noqa: E501
    assert nuthatch._nest_call('GET', '/api/watch')['watched'] == [str(ADDRESS).lower()]
    assert nuthatch._nest_call('POST', '/api/unwatch', {'address': str(ADDRESS)})['watched'] == []
    with pytest.raises(RemoteError, match='400'):
        nuthatch._nest_call('POST', '/api/watch', {'validator': 1})
