# Testing and Validation

## Test Layers

Unit tests:

- Deterministic behavior checks for routing, ack batching, store replay, strategy logic, and API parity aliases.

Integration tests:

- Environment-gated endpoint validation for connect/logon, pub/sub, SOW flows, queue ack behavior, bookmark resume, timer commands, and HA failover setup.

## Commands

```bash
make test
make integration-test
make release
```

Equivalent direct commands:

```bash
go test ./...
go test ./... -run Integration
```

## Integration Environment Contract

- `AMPS_TEST_URI`
- `AMPS_TEST_FAILOVER_URIS`
- `AMPS_TEST_PROTOCOL`
- `AMPS_TEST_USER`
- `AMPS_TEST_PASSWORD`

`AMPS_TEST_FAILOVER_URIS` accepts comma-separated URIs.

## Validation Checklist for Parity-Sensitive Changes

1. Confirm no exported API signature regressions.
2. Run full unit suite.
3. Run integration suite with target endpoint.
4. Validate handler order expectations.
5. Validate retry and replay behaviors under disconnect.
6. Validate queue auto-ack batching and timeout behavior.
7. Update parity matrix and relevant workflow docs.

## Link and Documentation Integrity

- Ensure every new API behavior has at least one workflow or reference entry.
- Ensure all relative links in `README.md` and `docs/*.md` resolve.

## Related

- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
- [Reference: Client](reference_client.md)
- [Reference: HAClient](reference_ha_client.md)
