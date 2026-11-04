# Gap Register

Behavioral review reference: **C++ 5.3.5.14**, verified again 2026-11-01.

The symbol mapping covers 261 entries. The public-declaration inventory covers 755 entries. Neither count establishes behavioral equivalence. See the [initial audit](behavior_parity_audit.md), [individual inventory](behavior_parity_inventory.md) and [second audit](behavior_parity_second_audit.md).

The source of truth for current open/closed state is `tools/parity_behavior_manifest.json`.

Current contract status: **OPEN_GAPS=0** across 55 entries. All 16 groups from the second audit and the three reopened general contracts have normal-suite regressions and [recorded corrections](behavior_parity_second_fixes.md). The 19 original diagnostic functions no longer require an overlay. Release validation is tracked separately in that correction report; a closed manifest does not establish behavioral equivalence or replace the gates. The original 23 groups retain their [fixes and regression tests](behavior_parity_fixes.md).

| ID | Priority/domain | Status | Contract |
| --- | --- | --- | --- |
| `header-fast-path-preserves-present-fields` | header serialization | closed | The Go publish fast path preserves the general writer's present fields, including numeric zero and empty text values. |
| `websocket-wire-framing` | transport | closed | AMPS WebSocket negotiation and multipart framing pass strict wire and independent JavaScript client checks. |
| `contract-parity-gating` | parity-contract | closed | Symbol presence and manually closed statuses do not establish behavioral equivalence; the second audit reproduces additional divergences against C++ 5.3.5.14. |
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
| `audit-d01-replay-live-publish-ordering` | P1 | closed | Una publicación concurrente adelanta al replay y su ack acumulativo elimina mensajes todavía no enviados. |
| `audit-d02-wal-torn-tail-append-restart` | P1 | closed | Una cola parcial ignorada al recuperar el WAL corrompe la siguiente escritura y el segundo reinicio. |
| `audit-d03-checkpoint-publish-acceptance` | P1 | closed | Publish devuelve error después de que el WAL retuvo el mensaje, induciendo reintentos ambiguos. |
| `audit-d04-hybrid-initial-sequence-floor` | P1 | closed | HybridPublishStore reemplaza el piso de secuencia de sus backends nuevos por un ack antiguo. |
| `audit-d05-decoded-header-routing` | P1 | closed | Los headers conservan escapes JSON y el sub_id decodificado no encuentra su ruta. |
| `audit-d06-ha-logon-deadline` | P1 | closed | HAClient.SetTimeout no limita el logon cuando el servidor acepta TCP y no responde. |
| `audit-d07-logon-metadata-data-race` | P1 | closed | Los getters de versión y name hash compiten con escrituras del logon. |
| `audit-d08-fakeamps-publisher-reconnect-dedupe` | P1 | closed | fakeamps olvida la secuencia del publisher al cerrar la conexión y aplica otra vez un replay idéntico. |
| `audit-d09-replay-persisted-ack-request` | P2 | closed | El store copia el comando antes de agregar persisted, por lo que el replay no solicita su confirmación. |
| `audit-d10-unsubscribe-recovery-cancellation` | P2 | closed | Un snapshot de recuperación puede recrear una suscripción después de que Unsubscribe terminó. |
| `audit-d11-stream-ack-failure-retry` | P2 | closed | HasNext olvida el mensaje previo cuando falla su ack y la siguiente llamada avanza. |
| `audit-d12-offline-execute-ack-handler` | P2 | closed | ExecuteAsync desconectado retiene el publish pero no su callback de acknowledgements. |
| `audit-d13-queue-handler-error-cancel` | P2 | closed | Un error del handler de cola deja el lease activo en vez de cancelarlo como C++. |
| `audit-d14-message-ack-autoack-guard` | P2 | closed | Message.Ack envía otra confirmación cuando AutoAck está habilitado, a diferencia de C++. |
| `audit-d15-json-builder-control-escapes` | P2 | closed | JSONMessageBuilder usa escapes de literales Go que producen JSON inválido para 27 controles ASCII. |
| `audit-d16-fakeamps-command-id-is-not-sequence` | P2 | closed | fakeamps descarta publicaciones distintas que comparten command ID. |

The native [C++/fakeamps gate](cpp_fakeamps_interop.md) adds these contracts:

| ID | Priority/domain | Status | Contract |
| --- | --- | --- | --- |
| `cpp-fakeamps-routing-bookmarks` | native C++ interoperability | closed | Route lists and escaped topics reach the official SDK; EPOCH, NOW and explicit bookmark resume select the expected history. |
| `cpp-fakeamps-queue-and-oof` | native C++ interoperability | closed | Manual queue ack, lease redelivery, retained reconnect and explicitly enabled OOF pass with the official SDK. |

Run the current reference check with:

```powershell
go run ./tools/paritycheck -headers .tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
pwsh -NoProfile -File tools/paritycheck/run_deep_audit.ps1
```

Expected result: `PARITY_ENTRIES=261`, both missing-symbol counts zero, `BEHAVIOR_ENTRIES=55`, `OPEN_GAPS=0`, and exit code 0. The normal regressions and release gates remain required; changing metadata alone cannot validate a contract.
