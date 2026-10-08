# Scanner test fixture

`block_results_failed.json` is one `block_results` answer from Mocha
(`mocha-5`, app version 10), saved byte for byte as the node returned it on
8 October 2026:

```
curl -s "https://rpc.celestia-mocha.com/block_results?height=1498161"
```

| tx | code | what it is |
|---|---|---|
| 0 | `sdk` 11 | out of gas after the ante handler passed: the `tx` event carries `fee` `213utia` |
| 1 | 0 | a successful MsgPayForBlobs |

`rpc-mocha.pops.one`, the public RPC the scanner's live test names, does not
keep `block_results` ("node is not persisting finalize block responses",
see deploy/README.md), so the answer comes from the public RPC that
`rpc-check.sh` is pointed at in deploy/README.md. The block was found by
comparing, over height ranges, the `tx_search` counts of `tx.fee_payer
EXISTS` and `message.action EXISTS`: a tx whose ante handler passed and
whose messages failed has the first and not the second.

`TestARealBlockResultsAnswerIsReadWhole` serves its `result` through the
fake RPC node and checks that the scanner reads every tx's codespace, log,
gas and ante fee event as the answer holds them.
