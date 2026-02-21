# Getting Started

## Scope

This guide covers connection setup, logon, and first command execution.

## Preconditions

- Go toolchain installed.
- Reachable AMPS endpoint.
- Import path: `github.com/Thejuampi/amps-client-go/amps`.

## Connection URI

Typical URI:

- `tcp://host:port/amps/json`

Credentialed URI:

- `tcp://user:password@host:port/amps/json`

## Minimal Lifecycle

1. Construct client.
2. Connect.
3. Logon.
4. Execute command(s).
5. Close.

```go
client := amps.NewClient("app")
if err := client.Connect("tcp://localhost:9000/amps/json"); err != nil {
	return err
}
defer client.Close()

if err := client.Logon(); err != nil {
	return err
}
```

## First Async Subscription

```go
_, err := client.SubscribeAsync(func(msg *amps.Message) error {
	_ = msg
	return nil
}, "orders")
```

## First Publish

```go
if err := client.Publish("orders", `{"id": 1}`); err != nil {
	return err
}
```

## Next Documents

- [Client Entrypoints](client_entrypoints.md)
- [Pub/Sub and SOW](pub_sub_and_sow.md)
- [Testing and Validation](testing_and_validation.md)
