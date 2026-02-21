# Reference: Command and MessageStream

## Command

Constructor:

- `NewCommand(commandName)`

Common getters:

- `AckType`, `BatchSize`, `Bookmark`, `Command`, `CommandID`, `CorrelationID`, `Data`
- `Expiration`, `Filter`, `Options`, `OrderBy`, `QueryID`, `SequenceID`
- `SowKey`, `SowKeys`, `SubID`, `SubIDs`, `TopN`, `Topic`

Common setters:

- `SetAckType`, `AddAckType`, `SetBatchSize`, `SetBookmark`, `SetCommand`, `SetCommandID`
- `SetCorrelationID`, `SetData`, `SetExpiration`, `SetFilter`, `SetOptions`, `SetOrderBy`
- `SetQueryID`, `SetSequenceID`, `SetSowKey`, `SetSowKeys`, `SetSubID`, `SetSubIDs`
- `SetTopN`, `SetTopic`

Notes:

- Setters are chainable.
- Command IDs are assigned by `Client.ExecuteAsync` when omitted.

## Message

`Message` provides parsed header/data access for inbound frames.

Common getters include:

- routing/context: `Command`, `CommandID`, `SubID`, `SubIDs`, `QueryID`, `Topic`
- ack/status: `AckType`, `Status`, `Reason`
- queue/bookmark: `Bookmark`, `LeasePeriod`
- data: `Data`, `MessageLength`, `SequenceID`

Utility:

- `Copy()` creates deep copy for safe retention outside handler call path.

## MessageStream

`MessageStream` is the synchronous iterator return type for `Execute(...)` and sync workflow helpers.

Core methods:

- `HasNext()`
- `Next()`
- `Close()`

Controls:

- `SetTimeout(ms)` / `Timeout()`
- `SetMaxDepth(depth)` / `MaxDepth()` / `Depth()`
- `Conflate()`

Notes:

- `Close()` handles unsubscribe/route cleanup based on stream state.
- `Next()` returns `nil` on completion or timeout transition.

## Related

- [Pub/Sub and SOW](pub_sub_and_sow.md)
- [Reference: Client](reference_client.md)
