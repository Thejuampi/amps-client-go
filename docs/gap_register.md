# Gap Register

Behavioral review reference: **C++ 5.3.5.14**, verified 2026-10-24.

The symbol mapping covers 261 entries. The public-declaration review covers 748 entries. Neither count establishes behavioral equivalence. See the [behavior audit](behavior_parity_audit.md) and [individual inventory](behavior_parity_inventory.md).

The source of truth for current open/closed state is `tools/parity_behavior_manifest.json`.

Current status: **OPEN_GAPS=0** for the registered contracts. All 23 audit finding groups are fixed and their 32 contract probes run in the ordinary test suite. See the [fix record](behavior_parity_fixes.md) for validation and remaining compatibility limits. This status does not replace the other release gates.

| ID | Priority/domain | Status | Contract |
| --- | --- | --- | --- |
| `contract-parity-gating` | parity-contract | closed | Symbol presence and manually closed statuses do not establish behavioral equivalence; the 2026-10-24 audit reproduces divergences against C++ 5.3.5.14. |
| `correctness-parser-builder-hardening` | correctness | closed | Composite/FIX/NVFIX and header/message parsing paths handle invalid ranges and malformed wire data without panics. |
| `logon-timeout-enforcement` | session | closed | LogonParams.Timeout is enforced during logon ack wait. |
| `transport-compression-parity` | transport | closed | tcp/tcps URI compression=zlib and Client.SetCompression(true) enable compressed transport for regular and HA clients while rejecting unsupported transports. |
| `wal-checkpoint-durability` | durability | closed | File-backed stores use append/checkpoint persistence with crash-safe recovery semantics. |
| `ha-recovery-ordering` | high-availability | closed | Recovery ordering is deterministic: publish replay, resubscribe, retry/deferred callbacks. |
| `ha-reconnect-completion-handoff` | high-availability | closed | A disconnect after recovery succeeds but before its worker releases ownership triggers another recovery. |
| `processed-ack-warning-lifecycle` | session | closed | A received processed ack stops its warning; a completed watcher cannot follow a replacement command, and fakeamps flushes acknowledgements before later queued data. |
| `kerberos-pluggable-authenticator` | auth | closed | Pure-Go Kerberos compatibility authenticator is available through a dedicated package and existing Authenticator contract. |
| `capi-ssl-stateful-emulation` | capi-ssl | closed | CAPI SSL compatibility has stateful handle/context validation and deterministic error behavior. |
| `perf-regression-gate` | performance | closed | Benchmark baseline and regression gate are available for hot-path operations. |
| `documentation-alignment` | documentation | closed | Parity acceptance and scope docs align with implemented behavior and gate commands. |
| `audit-r01-queue-ack-wire-options-batching` | P1 | closed | el ack de cola usa otro comando y pierde opciones y batching manual |
| `audit-r02-stream-depth-backpressure` | P1 | closed | MaxDepth pierde mensajes en lugar de aplicar contrapresión |
| `audit-r03-stream-deferred-autoack` | P1 | closed | un stream confirma la cola antes de que la aplicación la consuma |
| `audit-r04-bookmark-active-discard-recovery` | P1 | closed | el bookmark store confunde recibido, procesado y descartado |
| `audit-r05-duplicate-filter-before-handler` | P1 | closed | se detecta el duplicado después de entregarlo |
| `audit-r06-stored-publish-disconnect-contract` | P1 | closed | un publish guardado devuelve error, y desconectado ni se guarda |
| `audit-r07-subscription-recovery-generation` | P1 | closed | falta la generación de resuscripción introducida por C++ |
| `audit-r08-recovery-failure-propagation` | P1 | closed | Logon informa éxito aunque falle la resuscripción |
| `audit-r09-current-cpp-uri-options` | P2 | closed | Connect rechaza opciones válidas de la referencia actual |
| `audit-r10-unknown-header-exact-match` | P1 | closed | un campo desconocido puede modificar otro header |
| `audit-r11-publish-batching-effective` | P2 | closed | SetPublishBatching no modifica los envíos |
| `audit-r12-public-router-key-lookup` | P2 | closed | el MessageRouter público no encuentra sus rutas normales |
| `audit-r13-composite-empty-parts` | P2 | closed | se elimina una parte vacía de un composite |
| `audit-r14-nvfix-size-accounting` | P2 | closed | NVFIX Size cuenta un byte extra por campo |
| `audit-r15-server-error-classification` | P2 | closed | se pierden categorías de errores del servidor |
| `audit-r16-fresh-memory-publish-sequence` | P1 | closed | el memory publish store reinicia secuencias pequeñas |
| `audit-r17-logon-publisher-name-hash` | P2 | closed | GetNameHash toma otro dato del logon ack |
| `audit-r18-capi-ack-and-sequence-fields` | P1 | closed | CAPI descarta ack types de texto y confunde sequence con sub id |
| `audit-r19-named-server-version` | P2 | closed | versiones con nombre se interpretan como versión cero |
| `audit-r20-auth-retry-terminates-logon` | P1 | closed | un error de Retry del autenticador no termina Logon |
| `audit-r21-topn-zero-default-boundaries` | P2 | closed | TopN cambia los límites cero y default |
| `audit-r22-cpp-crc-algorithm` | P2 | closed | CRC es otro algoritmo |
| `audit-r23-hybrid-publish-sequence` | P1 | closed | HybridPublishStore puede repetir secuencias al cambiar de store |

Run the current reference check with:

```powershell
go run ./tools/paritycheck -headers .tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
```

Expected result for this audit: both missing-symbol counts are zero; OPEN_GAPS is zero. The ordinary suite includes the behavioral regression probes; symbol presence alone is still insufficient.
