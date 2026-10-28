# Correcciones de los 23 hallazgos de paridad

Referencia: C++ **5.3.5.14**, verificada el 2026-10-24. Los 23 grupos R01–R23
identificados en la [revisión inicial](behavior_parity_audit.md) están corregidos.
Sus **32 pruebas de diagnóstico** ahora forman parte de la suite ordinaria,
sin build tag, sin skips y sin cambiar sus contratos para aceptar los defectos.

Los [manifiestos](../tools/parity_behavior_manifest.json) cierran las 23 entradas
nuevas y las dos entradas generales reabiertas por la auditoría. El registro queda
con `OPEN_GAPS=0` para los contratos registrados. Las limitaciones C01–C08 de la
revisión siguen diferenciadas de esos contratos; no se certifica equivalencia
completa contra un servidor AMPS real.

## Correcciones y regresiones permanentes

| Grupo | Comportamiento corregido | Regresiones originales en la suite normal |
| --- | --- | --- |
| R01 | Acks sow_delete con opciones y batching; un envío fallido conserva el lote pendiente. | [TestParityAuditQueueAckCommand](../amps/behavior_parity_regression_test.go#L106), [TestParityAuditMessageAckOptions](../amps/behavior_parity_regression_test.go#L116), [TestParityAuditManualAcksRespectBatchSize](../amps/behavior_parity_regression_test.go#L128) |
| R02 | Contrapresión sin expulsar entradas; el dispatcher reintenta la entrega y atiende heartbeats. | [TestParityAuditStreamFullPreservesOldest](../amps/behavior_parity_regression_test.go#L139) |
| R03 | El stream difiere auto-ack hasta el avance del consumidor. | [TestParityAuditStreamDefersAutoAck](../amps/behavior_parity_regression_test.go#L151) |
| R04 | Descartes individuales, reentregas activas y frontera de recuperación segura; Log asigna la secuencia al Message. | [TestParityAuditBookmarkOutOfOrderDiscard](../amps/behavior_parity_regression_test.go#L11), [TestParityAuditBookmarkActiveRedelivery](../amps/behavior_parity_regression_test.go#L23), [TestParityAuditBookmarkResumeKeepsUnprocessed](../amps/behavior_parity_regression_test.go#L42), [TestParityAuditBookmarkLogSetsMessageSequence](../amps/behavior_parity_regression_test.go#L33) |
| R05 | Detección de duplicados antes del callback, separada por subscription id. | [TestParityAuditDiscardedBookmarkNotDelivered](../amps/behavior_parity_regression_test.go#L53) |
| R06 | Publish retenido sobrevive a desconexión y fallo de envío; no se confunde una secuencia explícita con retención. | [TestParityAuditStoredPublishSendFailure](../amps/behavior_parity_regression_test.go#L70), [TestParityAuditDisconnectedPublishIsStored](../amps/behavior_parity_regression_test.go#L84) |
| R07 | Generación por conexión evita repetir la suscripción en la misma sesión. | [TestParityAuditSubscribeRecoveryGeneration](../amps/behavior_parity_regression_test.go#L198) |
| R08 | Los errores de recovery llegan a Logon y conservan los retries/callbacks pendientes. | [TestParityAuditResubscribeFailurePropagatesFromLogon](../amps/behavior_parity_regression_test.go#L166) |
| R09 | Opciones URI aplicadas a dial, fallback IP, socket, preflight y logon pretty. | [TestParityAuditConnectTimeoutURI](../amps/behavior_parity_regression_test.go#L330) |
| R10 | Comparación exacta de claves; los campos desconocidos no pisan headers conocidos. | [TestParityAuditUnknownHeaderDoesNotOverwriteCommandID](../amps/behavior_parity_regression_test.go#L265) |
| R11 | Batching efectivo por tamaño/tiempo, con flush antes de comandos de control y aislamiento entre sesiones. | [TestParityAuditPublishBatching](../amps/behavior_parity_regression_test.go#L319) |
| R12 | Routing por los ids del mensaje, sin depender de una key privada vacía. | [TestParityAuditRouterAck](../amps/behavior_parity_regression_test.go#L232) |
| R13 | Una parte vacía conserva su prefijo de cuatro bytes. | [TestParityAuditEmptyCompositePart](../amps/behavior_parity_regression_test.go#L244) |
| R14 | Size coincide con los bytes NVFIX serializados. | [TestParityAuditNVFIXSize](../amps/behavior_parity_regression_test.go#L255) |
| R15 | Categorías de error específicas y conservación del reason del servidor. | [TestParityAuditErrorClassification](../amps/behavior_parity_regression_test.go#L275), [TestParityAuditReplayFailureIsClassified](../amps/behavior_parity_regression_test.go#L293) |
| R16 | Los stores nuevos de memoria y archivo fabrican la secuencia inicial; un checkpoint/WAL con historial conserva su contador. | [TestParityAuditFreshMemoryPublishSequence](../amps/behavior_parity_regression_test.go#L94) |
| R17 | El name hash se toma del publisher del bookmark del logon ack. | [TestParityAuditLogonNameHashComesFromBookmark](../amps/behavior_parity_regression_test.go#L176) |
| R18 | Ack types de texto preservados; sequence y sub_id usan campos diferentes. | [TestParityAuditSendPreservesTextAckTypes](../amps/capi/behavior_parity_regression_test.go#L9), [TestParityAuditSendPreservesSequence](../amps/capi/behavior_parity_regression_test.go#L16) |
| R19 | Builds con nombre usan capacidades 99999999. | [TestParityAuditNamedVersion](../amps/behavior_parity_regression_test.go#L299) |
| R20 | Errores del Retry/Continue del autenticador completan la espera de Logon. | [TestParityAuditAuthenticatorRetryErrorTerminatesLogon](../amps/behavior_parity_regression_test.go#L189) |
| R21 | TopN cero explícito; UINT_MAX limpia el campo. | [TestParityAuditTopNZeroIsExplicit](../amps/behavior_parity_regression_test.go#L305), [TestParityAuditTopNDefaultSentinelClearsField](../amps/behavior_parity_regression_test.go#L312) |
| R22 | CRC portátil de 64 bits con init cero y sin complemento final; Compute devuelve uint64. | [TestParityAuditCRCMatchesCPP](../amps/cppcompat/behavior_parity_regression_test.go#L10) |
| R23 | Secuencia coordinada y replay ordenado entre stores; una escritura parcial conserva la identidad del publish. | [TestParityAuditHybridSequencesRemainUniqueOnFallback](../amps/cppcompat/behavior_parity_regression_test.go#L19) |

Además de los probes originales, las nuevas pruebas cubren acks retenidos ante
fallos, contrapresión con errores envueltos, cero copias al reintentar una cola
llena, deduplicación por sub id, fallback y recuperación del store híbrido,
fallos posteriores a una escritura parcial, recuperación desde archivos, retención
de entradas activas en el ring, opciones de conexión y fronteras
de batching por tamaño, timeout, comandos de control y cambio de sesión.
La integración verifica el ack de cola serializado y la reanudación con mensajes
procesados y pendientes: sólo los pendientes se vuelven a entregar.

## Cambios que afectan a aplicaciones existentes

- `cppcompat.CRC.Compute` devuelve **uint64** y calcula el CRC del backend
  portátil C++ (`crcNoSSE`), con init cero y sin complemento final. El retorno
  anterior uint32 no podía representar ese resultado. Código con variables o
  interfaces tipadas uint32 debe actualizarse. No se selecciona otro algoritmo
  según el hardware SSE del host. Los golden values provienen del SDK compilado.
- El tercer argumento opcional de `Client.Ack` representa **opciones**, como
  `cancel`. El ack identifica mensajes mediante topic y bookmark. El método ya
  no transmite ese argumento como sub id.
- `SetTopN(0)` solicita explícitamente cero resultados; `UINT_MAX` limpia el
  límite. Los publish stores nuevos de memoria y archivo usan secuencias fabricadas basadas en tiempo.
- Un stream limitado conserva los mensajes y aplica contrapresión. Un consumidor
  debe avanzar o cerrar el stream para liberar el receive loop. Auto-ack de cola
  se realiza al avanzar después del mensaje entregado, como en C++. Si falla ese ack, no se avanza al siguiente mensaje.
- El ring conserva entradas activas aunque superen `RingSize`; su límite es un
  objetivo de retención de historial, no una autorización para perder pendientes.

No se cambia el formato nativo de los WAL/checkpoints ni el toolchain de release.
Se conserva la lectura de registros legacy `discard_upto`; los descartes nuevos
se registran individualmente. Una historia que ya fue guardada con descartes
incorrectos no permite reconstruir los mensajes omitidos.

## Validación inicial (2026-10-24)

| Control | Resultado |
| --- | --- |
| RED inicial | 32 contratos fallaban antes de implementar. |
| Regresiones originales ordinarias | 32 PASS, sin `-tags parity_audit`. |
| Suite completa | PASS: `go test ./... -count=1`, incluido fakeamps (124.7 s). |
| Integración estricta con fakeamps | PASS: integración del cliente y del broker, incluido stress de reconexiones. |
| Race detector `./amps/...` | PASS. |
| Coverage gate | PASS: 91.6% agregado (8408/9175); thresholds por archivo satisfechos. |
| Static scan | PASS: vet, staticcheck, ineffassign, errcheck y linters del repositorio. |
| Escaneo completo | BLOQUEADO: govulncheck encuentra nueve vulnerabilidades alcanzables de la stdlib de Go 1.25.13; gosec: 109 archivos, cero incidencias. |
| Secret scan | PASS: sin secretos detectados. |
| Paridad contra headers C++ 5.3.5.14 | PASS: 261 entradas, cero referencias faltantes, 33 contratos, OPEN_GAPS=0. |
| Compatibilidad Go 1.25.13 | PASS: compat-check completo y nueva ejecución de `./amps/...` tras los últimos cambios. |
| Performance gate Go 1.25.13 | PASS tras la repetición confirmatoria automática del gate; baseline original sin cambios. |
| Mutaciones reales mediante overlays | 16/16 eliminadas por assertions de las pruebas; ninguna depende de un error de compilación. |

Comandos para repetir las verificaciones funcionales:

```powershell
go test ./amps ./amps/capi ./amps/cppcompat -run '^TestParityAudit' -count=1
go test ./... -count=1
go test -race ./amps/... -count=1
make integration-fakeamps
make coverage-check
make static-scan
make scan
make perf-check
go run ./tools/paritycheck -headers .tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
```

El benchmark adicional `BenchmarkPublishBatching` mide el publish real inmediato
y con un lote de 4096 bytes, usando un transporte que descarta bytes para evitar
que el coste del socket o la acumulación de datos del fake distorsionen la medida.
La prueba `TestBoundedStreamDoesNotCopyWhileFull` exige cero allocs por intento
tanto en streams normales como conflados. No se cambia el baseline de performance.
En tres muestras de 300 ms, publish inmediato midió 94.8–95.4 ns/op, 0 B/op y
0 allocs/op; batching de 4096 bytes midió 101.9–104.3 ns/op, 1 B/op amortizado y
0 allocs/op reportadas. Este transporte sin coste de escritura mide overhead del
cliente; no demuestra una mejora de throughput de red.

Las mutaciones verificaron el comando del ack, auto-ack diferido, detección de
duplicados, envío retenido, generación de suscripciones, propagación de errores
de recovery, claves del header, batching, CRC, capacidad del stream, frontera de
bookmarks, secuencia tras escritura parcial, retención del ring, linger cero,
fallo del ack diferido y aislamiento entre intentos de suscripción.

El escaneo de vulnerabilidades conserva los hallazgos de toolchain ya señalados
en la auditoría: GO-2026-6809, GO-2026-6617, GO-2026-6613, GO-2026-6612,
GO-2026-6611, GO-2026-6608, GO-2026-6607, GO-2026-6604 y GO-2026-6603.
No se cambia ni se presenta como aprobado el gate de seguridad de release.

Una ejecución concurrente con los escáneres y las mutaciones falló en dos pruebas
de stress: espera de acks en una reconexión y un aviso de processed ack a 250 ms.
El gate estricto de integración y la suite completa ejecutados después pasaron
sin modificar los límites de esas pruebas.

## Seguimiento del stress (2026-10-26)

Se reprodujeron de forma determinista dos ventanas que generan los síntomas del
stress anterior. Las trazas históricas no registraban el instante de recepción
del ack ni el estado del worker, por lo que no permiten atribuir cada fallo
antiguo a una única causa. Una primera repetición de los dos stress originales
con race y shuffle también pasó antes de corregir; el fallo era intermitente.

- **Reconexión perdida al completar recovery.**
  `TestIntegrationHAReconnectAfterDisconnectAtSuccess` bloquea el callback
  `ReportSuccess` de la primera recuperación, corta la nueva conexión y espera
  a que se notifique esa desconexión antes de liberar el callback. Antes del
  cambio, el indicador `reconnecting` descartaba el segundo evento y el cliente
  quedaba desconectado. Ahora el worker libera su cancelación y su indicador
  bajo el mismo lock, y vuelve a solicitar recovery si la conexión ya cayó.
  La cancelación explícita de HA conserva su prioridad.
- **Aviso obsoleto de processed ack.**
  `TestProcessedAckReceiptStopsWarningsBeforeWaiterResumes` deja el ack recibido
  en su canal mientras el goroutine que lo espera todavía no lo consume. Antes
  aparecía el aviso a los 250 ms, incluso cuando el ack había llegado antes de
  armar el timer. Ahora la recepción se registra bajo el lock existente y el
  watcher comprueba la identidad de su espera. También se prueban el cierre,
  el reemplazo y que un ack anterior no silencie el aviso de la próxima orden.
  Las pruebas existentes de avisos por demora y backoff siguen activas.

La segunda vuelta del primer gate ampliado volvió a fallar con un ack pendiente
a los 250 ms (cliente nuevo, ciclo 42, command id 2). Se mantuvo ese resultado
rojo para continuar el diagnóstico. Se incorporaron trazas acotadas de envío y
recepción de comandos/acks y del estado de GC para futuros fallos.

También se reprodujo que el writer de fakeamps retenía un ack mientras drenaba
datos SOW ya encolados. `TestConnWriterFlushesAcknowledgementBeforeQueuedData`
comprueba el primer write antes de consumir esos datos; fallaba con el writer
anterior. Ahora los acks forman una frontera de flush en la cola FIFO. Se
verifica que no adelanten datos anteriores y que los datos normales conserven
el coalescing. No se cambia la capacidad de cola ni el tamaño de buffer.

El gate incluye ahora todos los paquetes (`STRESS_PKG=./...`) y repite ambas
suites de integración con dos brokers efímeros, race y shuffle. Se mantiene
`STRESS_COUNT=20` por defecto. Una falla corta la ejecución; no se vuelve a
intentar esa falla para convertir el resultado en verde.

La ampliación encontró problemas de aislamiento en los tests: el snapshot de
runtime omitía restaurar `effectiveConfig`, los fixtures del gate asumían un
directorio temporal externo y un test de bookmarks comprobaba el estado antes
del ack completed. Se restauró el estado y se sincronizó el test con el evento
correcto, conservando sus aserciones y tiempos de espera.

Se mantienen el aviso inicial a 250 ms, los 2 s por ciclo, 1 s para reconexión,
el presupuesto de 25 s, mínimo de 25 ciclos y máximo de 1000 iteraciones de los
stress existentes. Los valores de CI/nightly/release no cambian.

El gate completo se ejecutó con `STRESS_COUNT=2`, el valor existente de CI, y
`FUZZ_TMPDIR=G:/dev/caches/tmp/amps-client-go-stress-20261026`. Ese directorio
externo separa los archivos temporales de compilación del recorrido de gosec;
no cambia las condiciones del stress. Las 20 vueltas indicadas abajo son de
la fase de paquetes; las suites de integración completas se ejecutaron dos veces.

| Verificación del seguimiento | Resultado |
| --- | --- |
| RED: reconexión durante ReportSuccess | FAIL: no se iniciaba la segunda recuperación. |
| RED: ack recibido y waiter suspendido | FAIL: aviso obsoleto a los 250 ms en ambos órdenes del timer. |
| Reconexión determinista, race, CPUs 1/2/8 | PASS: 20 repeticiones por configuración, 60 en total. |
| Ciclo de avisos, race, CPUs 1/2/8 | PASS: 20 repeticiones por configuración. |
| Suite de paquetes con race/shuffle, 20 vueltas | PASS: todos los paquetes, incluido fakeamps (225.4 s). |
| Suite de paquetes con race/shuffle, Go 1.25.13, 20 vueltas | PASS: todos los paquetes, incluido fakeamps (236.1 s). |
| Gate completo con la configuración de CI (2 vueltas) | PASS: ambas suites con race y shuffle en las dos vueltas; segunda vuelta del broker: 476.1 s. |
| Suite completa y compatibilidad Go 1.25.13 | PASS: `go test ./... -count=1` y `make compat-check`. |
| Cobertura actual | PASS: 91.6% agregado (8426/9194), tras la salida rápida del stream. |
| Opciones y repetición del gate | 100% de cobertura en las dos funciones nuevas; errores, count, race y shuffle verificados. |
| Writer actualizado | Run: 90.9%; identificación de acks: 100%; FIFO y batching verificados. |
| Manifiesto actualizado | 35 contratos, 261 mappings, OPEN_GAPS=0 tras el gate ampliado. |
| Static scan y gosec | PASS: 110 archivos y cero incidencias de gosec. |
| Escaneo de vulnerabilidades | BLOQUEADO: las mismas nueve vulnerabilidades de la stdlib de Go 1.25.13. |
| Performance del seguimiento | PASS: `make perf-check`, Go 1.25.13, baseline intacto y sin reruns confirmatorios. |
| Mutaciones del seguimiento | PASS: 12/12 eliminadas por fallos de assertions, sin errores de compilación. |

El primer gate de performance del seguimiento falló en dequeue de streams.
`Next` tomaba el lock de ciclo de vida también en estados que no limpian rutas.
Se añadió una salida rápida para esos estados; los estados SOW/stats siguen
sincronizados y vuelven a comprobar su estado después de tomar el lock, para
conservar rutas de una configuración que los reemplazó. Las regresiones de
sincronización pasan con race y CPUs 1/2/8. Los benchmarks enfocados medidos
después quedaron en 39.0–53.7 ns/op (hot) y 59.3–68.2 ns/op (concurrente),
sin allocations reportadas. El baseline no se modifica.

El gate completo posterior pasó en su primera medición: medianas de 48.80 ns/op
en dequeue hot y 53.34 ns/op en concurrente, ambos con 0 allocs/op reportadas.
En tres muestras de 300 ms con Go 1.25.13, la recepción del sync ack midió
46.97–47.61 ns/op y la clasificación de acks de fakeamps 1.916–1.971 ns/op,
ambas con 0 B/op y 0 allocs/op. Estas medidas aisladas no representan throughput
de red.
