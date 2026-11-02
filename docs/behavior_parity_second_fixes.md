# Correcciones de la segunda revisión de comportamiento

Fecha: **2026-11-01**. Base revisada: `91dd4a8224dcdc30cd866b24a595fdbb988b2e5b`
(`release: v1.0.0`). Referencia: **C++ 5.3.5.14**, identificada en la
[segunda auditoría](behavior_parity_second_audit.md). Este documento registra
la corrección y la validación de D01–D16; el informe de detección conserva sus
resultados históricos sobre el commit base.

Estado de las correcciones D01–D16: regresiones, mutaciones y release completo
aprobados. La ejecución terminó el 2026-11-02 a las 00:18 UTC (2026-11-01 local),
antes de la optimización posterior de tiempos de la suite.

El release posterior, con la suite optimizada y el escape de topics SOW de
fakeamps corregido, también pasó: **34 min 23 s**, unas cinco veces más rápido.
El [desglose de tiempos](test_suite_timings.md) conserva los datos de ambas
ejecuciones, el perfil controlado y las condiciones de comparación.

El release 1.1.0 incorpora además el
[cliente C++ oficial contra fakeamps](cpp_fakeamps_interop.md), con cinco
grupos de correcciones del broker y diez escenarios nativos aprobados.
La ejecución estricta completa pasó en **37 min 5,7 s**, con las 20 rondas,
race, cobertura y performance originales. El registro de tiempos distingue
esta ejecución de las dos anteriores.

La [verificación formal acotada](formal_verification.md) añade cuatro modelos
TLA+, siete configuraciones normales, quince variantes defectuosas y cinco
mutaciones vinculadas al Go real. El informe distingue las propiedades
modeladas de los grupos que siguen verificados mediante tests y race.

## Contratos y regresiones

Las 19 funciones diagnósticas originales se trasladan a archivos `_test.go`
ordinarios. Las pruebas independientes añaden combinaciones y caminos de
fallo que los primeros diagnósticos no cubrían. No se requiere un overlay
para ejecutar estas regresiones.

| Grupo | Contrato corregido | Regresiones principales en la suite normal |
| --- | --- | --- |
| D01 | La retención y el envío de comandos secuenciados respetan el orden del replay. La recuperación no habilita publicaciones de una conexión posterior. | `TestDeepAuditLivePublishCannotOvertakeRecovery`, `TestDeepAuditIntegrationLivePublishDuringReplayLosesBacklog`, `TestFollowupLoggedOnCallbackCannotOvertakeReplay`, `TestFollowupSOWDeleteCannotFallBehindReplaySnapshot`, `TestDeepAuditReplayCannotCompleteAfterConnectionChanges`. |
| D02 | La recuperación writable recorta una cola WAL incompleta antes del siguiente append. Un registro completo corrupto continúa siendo un error. | `TestDeepAuditPublishWALTornTailThenAppendThenRestart`, `TestDeepAuditBookmarkWALTornTailThenAppendThenRestart` y pruebas de `amps/internal/wal`. |
| D03 | Una publicación retenida no se informa como rechazada por un fallo posterior de mantenimiento. Un rechazo sin WAL no deja una copia para el siguiente checkpoint. | `TestDeepAuditPublishCheckpointFailureIsNotRejectedAfterRetention`, `TestDeepAuditCheckpointFailureReportsMaintenanceAfterAcceptance`, `TestFollowupCheckpointOnlyRejectionDoesNotRetainRetryDuplicate`, `TestFollowupNonzeroSequenceDoesNotHideStoreFailure`. |
| D04 | Hybrid conserva el piso de secuencia de sus backends nuevos y coordina la identidad del mensaje al cambiar de store. | `TestDeepAuditFreshHybridDoesNotReuseOldPublisherSequences` y regresiones de fallback en `amps/cppcompat`. |
| D05 | Los getters y el ruteo usan el texto decodificado del header JSON; copiar el mensaje conserva texto y payload. El camino sin escapes conserva su bajo costo. | `TestDeepAuditReceivedHeaderUnescapesText`, `TestDeepAuditEscapedSubscriptionIDRoutes`, `TestFollowupDecodedHeaderCopyPreservesPayloadAndText`, `TestFollowupHeadersRejectInvalidStringEscapes`. |
| D06 | El presupuesto de conexión HA incluye logon y los acks de recuperación. La cancelación interrumpe el preflight HTTP sin cerrar otra generación. Un fallo terminal detiene la recuperación; un worker antiguo no reemplaza ni bloquea una sesión posterior. | `TestDeepAuditHATimeoutIncludesLogon`, `TestFollowupHTTPPreflightObservesConnectCancellation`, `TestFollowupHATimeoutIncludesResubscriptionAck` y las regresiones de lifecycle HA. |
| D07 | La lectura de versión y name hash está sincronizada con la actualización de logon. | `TestDeepAuditLogonMetadataConcurrentReaders`, ejecutada con `-race`. |
| D08 | fakeamps reconoce la identidad del publisher entre conexiones y reconstruye su secuencia al reabrir el journal de disco. | `TestDeepAuditFakePublisherDedupSurvivesReconnect`, `TestFollowupPublisherDedupSurvivesDiskJournalReload`. |
| D09 | El snapshot retenido incluye la solicitud de ack persisted. El replay añade esa solicitud a entradas antiguas que no la tenían, conservando identidad, payload y acks del usuario. | Los seis casos de `TestDeepAuditReplayPreservesPersistedAck` y los 18 casos de `TestFollowupLegacyPublishBacklogRequestsPersistedAck`. |
| D10 | Una resuscripción valida que el snapshot siga vigente antes de enviarse; cancelar una suscripción impide que ese snapshot la recree. | `TestDeepAuditUnsubscribeDuringRecoveryIsNotResurrected` y regresiones de grupos de resume. |
| D11 | Si falla el ack al avanzar un stream, se conserva la obligación de confirmarlo; reintentar un lote ya encolado no duplica su bookmark. Disconnect conserva los acks pendientes y recovery los envía sin necesitar otro mensaje. | `TestDeepAuditStreamAckFailureCannotBeBypassedByNextCall`, `TestDeepAuditStreamAckRetriesQueuedBatchWithoutDuplicating` y regresiones de ack tras Disconnect. |
| D12 | Un comando retenido conserva su handler de acks a través de la desconexión y elimina su ruta al completar las confirmaciones solicitadas. | `TestDeepAuditOfflineExecutePreservesAckHandler`, `TestDeepAuditRetainedAckHandlerSurvivesDisconnectAndCleansUp`. |
| D13 | Un handler de cola que devuelve error solicita la cancelación del lease bajo AutoAck y conserva los errores de envío. | `TestDeepAuditFailedQueueHandlerCancelsLease` y casos negativos de auto-ack. |
| D14 | `Message.Ack` respeta la responsabilidad de AutoAck; el ack manual sigue disponible cuando AutoAck está deshabilitado. | `TestDeepAuditMessageAckDefersToAutoAck` y las regresiones de ack manual anteriores. |
| D15 | JSONMessageBuilder emite escapes JSON válidos para claves, valores, controles y Unicode. | `TestDeepAuditJSONBuilderProducesValidJSONForControls`, `TestDeepAuditJSONBuilderRoundTripsKeysAndUnicode`. |
| D16 | Command ID correlaciona respuestas; fakeamps no lo usa para descartar publicaciones o SOW deletes distintos. | Los dos casos de `TestDeepAuditFakeCommandIDIsNotPublishIdentity`, `TestFollowupCommandIDDoesNotSuppressDistinctSOWDeletes` y replay de SOWDelete por secuencia. |

Las regresiones están en [amps](../amps/deep_audit_probe_test.go),
[cppcompat](../amps/cppcompat/deep_audit_probe_test.go),
[fakeamps](../tools/fakeamps/deep_audit_probe_test.go) y en las pruebas
independientes de [cliente](../amps/deep_audit_followup_test.go) y
[broker](../tools/fakeamps/deep_audit_followup_test.go). Los casos adicionales de
[secuencia](../amps/deep_audit_sequence_test.go) y
[fallos de disco](../tools/fakeamps/deep_audit_extra_test.go) también son tests
ordinarios, junto con los de
[reentrancia del autenticador](../amps/deep_audit_authenticator_test.go) y
[replay de comandos prestados por stores personalizados](../amps/deep_audit_legacy_replay_test.go).
Las pruebas de [lifecycle HA](../amps/ha_lifecycle_regression_test.go) cubren
callbacks tardíos, llamadas en cola y entrega de la recuperación entre intentos.

## Revisión independiente después de las primeras correcciones

Se revisaron los cambios del subagente y se añadieron reproducciones propias
antes de pedir las correcciones adicionales:

| Escenario adicional | Resultado RED observado | Invariante exigida |
| --- | --- | --- |
| Publicar desde el callback LoggedOn con backlog pendiente | Wire `[3,1,2,3]`. | El callback puede retener el mensaje sin adelantarlo ni bloquear el recovery. |
| Publicar concurrentemente mientras Logon instala el piso del servidor | Wire `[1]` después de un ack con secuencia 100. | Instalar el piso y habilitar publicaciones es una transición indivisible; el mensaje nuevo usa 101. |
| Cancelar Connect durante un preflight HTTP silencioso | La llamada no terminaba dentro de 300 ms. | El contexto controla también esta fase del transporte. |
| Rechazo de checkpoint sin WAL y reintento | La reapertura recuperaba dos publicaciones en lugar de una. | Un rechazo confirmado no se guarda más tarde junto con su reintento. |
| Copiar headers escapados y aceptar escapes inválidos | Texto sin decodificar; se aceptaban `\q`, `\x00` y `\u00xz`. | El texto lógico y el payload sobreviven a Copy; escapes inválidos producen error. |
| Reabrir el journal de fakeamps y reenviar la misma secuencia | Dos entradas en el journal en lugar de una. | La identidad de publicación se recupera junto con su entrada durable. |
| Ejecutar SOWDelete después del snapshot del replay | Wire `[1,2,4]`. | También los deletes retenidos participan del orden de secuencias. |
| Reabrir el journal con cola incompleta, publicar y reabrir otra vez | Sólo se recuperaba una de dos publicaciones aceptadas. | La reparación precede al append; un registro completo corrupto bloquea el almacenamiento. |
| Logon correcto seguido de un ack de resuscripción silencioso | Timeout HA de 40 ms bloqueado todavía a los 300 ms. | El mismo presupuesto limita la recuperación; cancelarlo no cierra una conexión nueva. |
| Dos SOW deletes distintos con el mismo command ID | Quedaba una de las dos filas. | El command ID no sustituye la identidad de secuencia. |
| Reabrir SOW y journal de disco y reenviar un delete ya confirmado | El replay borraba una fila más nueva recuperada del snapshot SOW. | La secuencia de delete también sobrevive a la reapertura y no aparece como publicación en el replay. |
| Recuperar backlog creado antes de corregir D09 | Los 18 casos de memoria, WAL y checkpoint enviaban acks 0/4 en lugar de 8/12. | El replay solicita persisted también al actualizar desde una versión anterior, sin renumerar ni alterar el contenido. |
| Timeout de resuscripción seguido de otra prueba de transporte | El stress con semilla `1793558016446297700` detectó un race: un reconnect independiente leía el dialer mientras el siguiente test lo reemplazaba. | El intento HA activo controla su recuperación y un fallo terminal no deja otro intento independiente. |
| Callback de desconexión después del error terminal de ConnectAndLogon | Una reproducción independiente sin red emitía Reconnecting después del error. | Un callback tardío no reinicia un intento que ya terminó con error. |
| Listener Reconnecting que restaura la sesión | Quedaba reservado un worker obsoleto después del éxito del listener. | La sesión recuperada se conserva sin otro intento ni cambio de estado. |
| Worker viejo cancelado y desconexión de una sesión nueva | No llegaba el segundo intento de la sesión nueva. | La salida del worker viejo no pierde una solicitud de reconexión vigente. |

Además se revisó el checkpoint Windows: el archivo temporal debe sincronizarse
antes del rename. `TestMMapCheckpointSyncFailurePreservesWALAndOldCheckpoint`
inyecta un fallo de Sync y exige conservar tanto el checkpoint anterior como
el WAL. Un fallo posterior a escribir el WAL necesita distinguir rollback
confirmado de resultado incierto; un store en estado incierto no debe admitir
otra publicación ni sobrescribir la evidencia con un checkpoint.

La revisión del subagente añadió otros contraejemplos: el callback LoggedOn
publicaba antes de instalar un piso de secuencia mayor recibido del servidor;
un rechazo del store dejaba una ruta y tracking de failed-write; Disconnect
eliminaba acks pendientes. Las correcciones instalan el piso antes de callbacks,
limpian exclusivamente el comando rechazado y conservan los acks hasta su envío.

## Alcance de las garantías

Un error de mantenimiento posterior a la aceptación se comunica por el handler
de excepciones del cliente; el mensaje conserva su secuencia y su retención.
Esto evita sugerir un nuevo Publish como reintento de una aceptación ya hecha.
Un error de store sin la marca explícita de aceptación continúa rechazando el
comando, aunque devuelva una secuencia no nula.

`SetPublishStore` serializa la reconfiguración con publicaciones y replay.
Un store personalizado no debe invocar ese setter desde su propio `Store` o
`Replay` en el mismo cliente: esperaría la operación que está ejecutando.
Los callbacks de LoggedOn/PublishReplayed y el handler de mantenimiento añadido
por estas correcciones se invocan fuera de esos locks. El
C++ de referencia rechaza `setPublishStore` estando conectado
(`ampsplusplus.hpp:3064`); la reconfiguración en vivo sigue siendo una extensión
Go y no una garantía compartida con C++.

La instalación del piso de secuencia conocido queda serializada con Store.
Los mensajes retenidos antes de recibir ese piso conservan su identidad;
no se renumeran retroactivamente. Los callbacks Retry/Continue del autenticador
pueden seguir reteniendo publicaciones durante logon. La prueba concurrente
del piso controla la instalación, no presupone que el cliente conozca la
secuencia del servidor antes de recibirla.

La recuperación de una cola WAL incompleta y los errores de Sync se prueban
con archivos e inyección de fallos. No equivalen a haber cortado físicamente la
energía del equipo. Tampoco permiten prometer un resultado cierto cuando fallan
la escritura y su rollback durable.

fakeamps guarda los watermarks de delete como registros tipados que no se
entregan como publicaciones al hacer replay. Se prueban la reapertura del
journal y SOW, así como el rechazo previo al delete si falla la escritura del
journal. El journal y el snapshot SOW son archivos separados: no forman una
transacción atómica ante una caída entre el registro y la actualización SOW.
Esta limitación también afecta a las publicaciones; la reapertura probada no
equivale a fidelidad completa frente a un corte de energía.
Los registros anteriores sin identidad siguen siendo legibles, pero no permiten
reconstruir una secuencia de publisher que nunca se guardó. La extensión del
formato ofrece lectura de archivos anteriores; no se garantiza que un binario
antiguo pueda leer los nuevos registros tipados.

Las integraciones usan fakeamps. El cotejo C++ incluye inspección de fuente y
el oracle de strings ya descrito en la auditoría; no se ejecuta el cliente C++
completo contra un servidor AMPS real. Cerrar D01–D16 no certifica equivalencia
de cada declaración del inventario.

## Ejecución de la suite en Windows

Los once constructores de procesos de fakeampsgate, CLI/gofer y las herramientas
de toolchain/performance usan [hiddenprocess.HideConsole](../internal/hiddenprocess/hidden_windows.go).
Se conserva SysProcAttr y se activa CREATE_NO_WINDOW quitando los flags
CREATE_NEW_CONSOLE y DETACHED_PROCESS, con los que Windows lo ignora. Las demás
plataformas conservan el comportamiento anterior.

Las regresiones comprueban atributos existentes, tres mutaciones de flags y
procesos reales: tanto hijo como nieto devuelven GetConsoleWindow()==0.
Un guard de wiring revisa los once constructores antes de usarlos. Pasaron
las integraciones dirigidas con fakeamps, race, Go 1.25.13 y compilación Linux.
La adaptación externa de statechart también configura workers, janitor y TLC
ocultos y evita detached en Windows. Se corrigió además la normalización CRLF.

El supervisor externo conserva stdout, stderr, argv, entorno y exit status al
ocultar procesos; tiene pruebas para esos contratos. Un exit status 0 sin el
log esperado no se cuenta como gate aprobado.

## Verificación final

La revisión independiente ejecutó las regresiones con `-race -cpu 1,2,8
-count=3` y obtuvo PASS en los tres paquetes. Las regresiones nuevas de HA
pasaron además con esas CPUs, race y cinco repeticiones; la reproducción
dirigida de la semilla fallida pasó con veinte repeticiones.
Se aplicaron diecinueve mutaciones
reales mediante overlays temporales, sin modificar las fuentes del checkout;
las diecinueve fallaron por aserciones de comportamiento, no por compilación.

| Mutación | Prueba que la detectó |
| --- | --- |
| Quitar el gate de replay | `TestDeepAuditLivePublishCannotOvertakeRecovery`. |
| Desactivar reparación de cola WAL | Ambas pruebas `TestDeepAudit*WALTornTailThenAppendThenRestart`. |
| Omitir persisted antes de Store | `TestDeepAuditReplayPreservesPersistedAck`. |
| Aceptar snapshot de suscripción obsoleto | `TestDeepAuditUnsubscribeDuringRecoveryIsNotResurrected`. |
| Invertir la guarda AutoAck | `TestDeepAuditMessageAckDefersToAutoAck`. |
| Omitir la liberación del publisher después de un fallo de journal | `TestDeepAuditUnavailableJournalRejectsWirePublishAndReleasesPublisher`. |
| Quitar la serialización al instalar el piso de secuencia | `TestFollowupConcurrentPublishWaitsForServerSequenceFloor`. |
| Omitir la restauración de secuencias de delete desde el journal | `TestFollowupSequencedDeleteSurvivesDiskRestart`. |
| Omitir el límite de identidad del publisher en PUB1 | `TestDeepAuditDiskPublisherIdentityLengthBoundaries`, casos de publicación. |
| Omitir el límite de identidad del publisher en DEL1 | `TestDeepAuditDiskPublisherIdentityLengthBoundaries`, casos de delete. |
| Invertir la normalización del ack en backlog antiguo | `TestFollowupLegacyPublishBacklogRequestsPersistedAck`, 18 casos. |
| No registrar el disconnect durante un intento HA activo | `TestHAInitialRecoveryFailureDoesNotStartCompetingReconnect`. |
| No detener HA al terminar con error | `TestFollowupHATerminalConnectFailureRejectsLateDisconnect`. |
| Aceptar una llamada HA anterior a Disconnect | `TestHAStaleQueuedAttemptCannotOverrideNewSession`, sesión nueva primero. |
| Borrar el disconnect pendiente antes de salir el worker anterior | `TestHACancelledOldWorkerHandsOffNewSessionDisconnect`, durante éxito. |
| Ignorar el disconnect con otro worker todavía reservado | `TestHACancelledOldWorkerHandsOffNewSessionDisconnect`, después del éxito. |
| No entregar un disconnect pendiente al salir del worker | `TestHACancelledOldWorkerHandsOffNewSessionDisconnect`. |
| Permitir que un worker en cola reemplace la sesión recuperada | `TestHAReconnectWorkerDoesNotReplaceConcurrentSession`. |
| Notificar Reconnecting sin liberar el lock del intento | `TestHAReconnectNotificationDoesNotHoldAttemptLock`. |

La primera versión de la prueba del worker en cola dejó sobrevivir su mutación:
otra guarda intervenía antes. Se movió la barrera al chooser, antes del dial,
para exigir que el worker quedara efectivamente en cola. La mutación entonces
produjo dos intentos y `stopped=true`; el código correcto conserva un solo
intento y la sesión activa. No se modificó la producción para hacer pasar esa
prueba ni se contabilizó como detectada antes de fortalecerla.

Un experimento adicional que fuerza la copia superficial también en el replay
moderno no fue detectado por el presupuesto de allocations: esa copia puede
quedar en el stack. No se cuenta como una mutación detectada ni como prueba de
ausencia de copias. `TestDeepAuditReplayModernAllocationBudget` verifica el
techo de una allocation en Go 1.25.13 y 1.26.10; la latencia se mide por separado
con `BenchmarkPublishReplayAcknowledgements`.

Se ejecutó ese benchmark sin otros gates en paralelo, con Go 1.26.10 en
Windows amd64, cinco muestras de un segundo. Medianas:

| Replay | Código final ns/op | Copia forzada ns/op | B/op, ambas variantes | allocs/op, ambas variantes |
| --- | ---: | ---: | ---: | ---: |
| Moderno, persisted ya solicitado | 161,1 | 170,5 | 24 | 1 |
| Backlog antiguo, necesita persisted | 169,7 | 169,5 | 24 | 1 |

El experimento cambia sólo la condición que evita copiar el header moderno.
Estas muestras muestran su costo temporal sin una allocation adicional; no
sustituyen el gate ni establecen un baseline nuevo.

Se midió también `BenchmarkPublishBatching`, porque el benchmark del gate
`BenchmarkPublishSendFull` sólo mide serialización y no todo `PublishBytes`.
Medianas de cinco muestras de un segundo en Windows amd64, Go 1.26.10:

| Variante sin PublishStore | Immediate ns/op | Batch4096 ns/op | allocs/op |
| --- | ---: | ---: | ---: |
| Primera corrección, con lock extra incondicional | 100,20 | 108,50 | 0 |
| Experimento temporal que quitaba exclusivamente ese lock | 92,22 | 100,10 | 0 |
| Implementación final, con sincronización condicional segura | 92,97 | 100,10 | 0 |

El experimento identifica el costo del lock; no se usa como baseline de release
ni como prueba de corrección. La implementación final conserva la serialización
con store y evita ese costo sin store. El gate completo de performance utiliza
el baseline existente.

| Gate o comprobación | Resultado |
| --- | --- |
| Regresiones nuevas, race, CPUs 1/2/8, tres vueltas | PASS. |
| Mutaciones de comportamiento sobre la corrección final | 19/19 detectadas por aserciones; el experimento adicional de copia en stack no fue detectado por allocations. |
| Verificación formal acotada | PASS: 1.254 estados distintos, 15 variantes defectuosas detectadas y 5 overlays Go; estos overlays reutilizan casos de la revisión de mutaciones anterior. |
| `make scan` sobre las fuentes finales | PASS: gosec 0 problemas; govulncheck 0 vulnerabilidades alcanzables y 0 en paquetes importados, con 2 avisos de módulos requeridos cuyo código vulnerable no se usa según el análisis; gitleaks sin secretos. |
| `make compat-check`, toolchain exacto Go 1.25.13 | PASS: tests y build. |
| `make coverage-check` dentro del release final optimizado | PASS: agregado 92,1% (8929/9698), con los mínimos existentes por archivo. |
| C++ actual 5.3.5.14 | PASS: 261 símbolos, 0 faltantes, 53 contratos, 0 gaps abiertos. |
| Inventario AST de declaraciones públicas | PASS: 753 declaraciones, incluidas cinco nuevas de soporte Go. |
| Oracle de strings extraído del SDK | PASS: barra invertida, comilla y salto de línea. |
| Compilación Linux amd64, Go 1.26.10 | PASS: todos los paquetes y compilación de tests de `amps`; no se ejecutaron en Linux. |
| `make release` completo | PASS, exit 0 y marcador final: scan, leak, fuzz, stress de veinte rondas de ambos paquetes, cobertura, performance, tests, race, build, integración y paridad. Pasada 1.1.0: 37 min 5,7 s, con gate C++ nativo, dos workers aislados y 319 fuentes/configuraciones verificadas sin cambios, incluidos 280 archivos Go. |
| Suite ordinaria `go test ./...` sobre la corrección final | PASS en todos los paquetes; incluye las integraciones del CLI. |
| Stress adicional de integraciones del CLI y del runner | PASS final: veinte repeticiones con race y shuffle, `-run 'Integration\|Gate'`; CLI 69,836 s y runner 1,262 s. Las unidades del runner también están incluidas en el stress unitario normal. |
| Suite general `go test -race -count=1 ./...` | PASS en todos los paquetes, además de las vueltas y suites separadas del release. |
| Cobertura final de fakeamps tras la optimización | PASS: suite completa con `-count=1 -coverprofile`, 80,4%. Las funciones nuevas del coordinador y el helper Windows tienen 100% de cobertura de sentencias; eso no sustituye sus regresiones y mutaciones. |

El baseline de performance mantiene SHA-256
`64db5a6cdee1ad878ba73c6a2b305fc14d10c1874b93cdd3f541d0318b515539`.
Los límites de rendimiento, allocations, cobertura, stress y timeouts no cambian.

El comando de release usa `FUZZ_TMPDIR=G:/dev/caches/tmp/amps-client-go-second-fixes`
para ubicar los temporales fuera del árbol que escanea gosec. No se modifica
`STRESS_COUNT=20`, `STRESS_SHUFFLE=on` ni `FUZZTIME=5s`. Las integraciones de stress
incluyen tanto `./amps` como `./tools/fakeamps` en cada vuelta.
