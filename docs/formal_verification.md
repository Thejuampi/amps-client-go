# Verificación formal acotada de recuperación AMPS

Creación: 2026-11-01. Última sincronización: 2026-11-03, sobre el commit
`333efdb62a74cbf5187e7a625b7e25e025b80920`. Todas las fuentes cubiertas están
commiteadas y coinciden con [source-sync.json](../formal/source-sync.json).

Se aplicó el flujo de pstack `create-formal-verification-skill`, su control de
deriva `maintain-formal-verification-skill` y la interpretación `statechart`.
La entrada `formal-check` es una adaptación local para Codex: no existe con
ese nombre en el upstream descargado.

La instalación local usa pstack 0.15.36 del repositorio cursor/plugins,
revisión `ae5650febf4320793844c12c4adaa937f417f8da`, conservando su licencia
MIT. Está en `C:/Users/Juan/.codex/pstack-codex-0.15.36`, con 59 skills
enlazadas desde `.agents/skills` del usuario. El adapter traduce herramientas y
recursos de Cursor a Codex; hereda el modelo actual y desactiva los paneles
automáticos de modelos. Los cambios de Windows en statechart tienen tests
para cierre de procesos, CRLF y lanzamiento sin ventanas.

## Resultado y alcance

TLC terminó las siete configuraciones normales sin violaciones, con **1.254
estados distintos en total**. Las quince variantes defectuosas violaron la
invariante esperada. Cinco mutaciones correspondientes sobre el Go real fueron
detectadas por aserciones; antes de cada mutación pasó la regresión original.
Los overlays no modifican las fuentes del checkout.

| Modelo/configuración | Estados distintos | Propiedades comprobadas |
| --- | ---: | --- |
| PublishRecovery | 503 | Un mensaje aceptado permanece retenido o aplicado; cada envío solicita persisted; un replay viejo no escribe en otra generación. |
| StoreRecovery | 14 | Lo aceptado sigue recuperable; un rechazo no reaparece; un resultado incierto conserva su evidencia. |
| HAHandoff, intento inicial | 26 | El intento controla su recuperación y el fallo terminal rechaza callbacks tardíos. |
| HAHandoff, worker anterior | 53 | El worker que sale conserva y entrega una reconexión de la sesión nueva. |
| HAHandoff, listener reentrante | 606 | Un worker reservado o en cola conserva la sesión que otro intento ya recuperó. |
| HAHandoff, llamada anterior | 30 | Una llamada de un epoch anterior se rechaza antes de tocar la nueva sesión. |
| BrokerDedup | 22 | Repetir publish/delete conserva la identidad después de una reapertura normal; el delete viejo no borra la fila nueva. |

Estos números describen los grafos alcanzables de los modelos. No son cantidad
de mensajes ensayados, cobertura del Go, ni una demostración de equivalencia
Go/C++. El hash del modelo y los parámetros exactos quedan en cada resultado.
Tampoco modelan los bytes, el handshake o la fragmentación WebSocket, ni prueban
el comportamiento de un servidor AMPS real.

Para publicación, si A son los identificadores aceptados, R los retenidos y B
los aplicados por el broker, se comprueba:

```text
A ⊆ R ∪ B
```

Para el store con WAL se exige:

```text
A ⊆ checkpoint ∪ prefijo válido del WAL
rechazado ⇒ el comando no está en memoria ni en el estado recuperable
incierto ⇒ store bloqueado y evidencia sin modificar hasta reabrir
```

La obligación de HA se comprueba como propiedad de seguridad: cuando una sesión
requiere reconexión y no queda intento ni worker, debe existir un callback o
una entrega pendiente. No se infiere que una red silenciosa finalmente responda.

## Cotas y supuestos

- PublishRecovery: tres identidades de mensaje, dos generaciones, un mensaje
  legacy inicialmente retenido, secuencias crecientes, snapshots ordenados y
  como máximo un recovery por generación. El último transporte se mantiene
  conectado para cerrar el espacio. Desconectar puede perder envíos y acks
  en tránsito. Las publicaciones usan el cliente; se excluye modificar el
  store directamente desde otra instancia o un callback personalizado.
- StoreRecovery: dos registros, una cola incompleta, un append y un
  checkpoint. Se distinguen append confirmado, rollback confirmado y rollback
  incierto. Se estudia la opción con WAL; no se verifica formalmente el store
  sin WAL, mmap, el sistema de archivos ni la durabilidad física de Sync.
- HAHandoff: cuatro estados iniciales alcanzables de las regresiones, un
  intento explícito nuevo, un worker anterior/reservado, una pérdida de socket
  y un callback tardío. Se separa comprobar el listener de programar el worker;
  la pérdida de conexión puede intercalarse. Las transiciones de ownership
  representan las secciones protegidas por ha.lock y connectAndLogonLock y la
  entrega local posterior al unlock. No se modelan duración de I/O, tiempo
  físico, autenticadores arbitrarios ni un número ilimitado de reconexiones.
- BrokerDedup: dos publishers nombrados, un delete, una publicación posterior,
  una reapertura normal y hasta dos replays de cada comando. Las dos operaciones
  iniciales terminan en ese orden. Se excluye la caída entre actualizar journal
  y SOW, que son archivos diferentes. No se verifican CRC, bytes del formato,
  identidades anónimas ni concurrencia ilimitada entre publishers.

Son abstracciones revisadas contra las funciones siguientes. No hay una prueba
mecánica que refine el programa Go completo hacia TLA+. Las operaciones de
locks, red, disco y runtime conservan los contratos indicados como supuestos.
Los errores de construcción del modelo se corrigieron como errores de
verificación; no se contaron como defectos del cliente.

## Correspondencia con el código

| Transiciones/estado | Fuente y correspondencia |
| --- | --- |
| Publish, StartReplay, SendReplay, FinishReplay, AbortReplay | [client.go](../amps/client.go): lockForPublish, publishBytesWithCommand y executeAsync; [client_parity_methods.go](../amps/client_parity_methods.go): storePublishCommand y replayPublishStore. publishLock excluye retenciones nuevas del snapshot activo. client.lock protege awaitingReplay y la validación de generación. |
| Deliver y PersistedAck | El broker aplica y confirma; DiscardUpTo de [publish_store.go](../amps/publish_store.go) elimina el prefijo confirmado. El modelo separa el envío, la aplicación y la recepción del ack. |
| Open, StartAppend, RollbackAppend, UncertainAppend | [wal.go](../amps/internal/wal/wal.go): ReplayAndRepair y Append. 0 representa bytes de un registro incompleto, no una secuencia válida. |
| CommitAppend, CheckpointFailure, AttemptMaintenance, Reopen | [publish_store.go](../amps/publish_store.go): Store, saveCheckpointLocked y load. PublishStoreAcceptedError distingue aceptación de rechazo y loadErr bloquea mutaciones inciertas. |
| HandleNotification, CheckNotification, ScheduleWorker, RunWorker | [ha_client.go](../amps/ha_client.go): handleDisconnect y entrada de connectAndLogonAttempt. El modelo separa callback externo, lectura de sesión y ejecución del worker. |
| CompleteSuccess, TerminalFailure, FinishWorker, DispatchHandoff | Defer de connectAndLogonAttempt y finishReconnect. pendingReconnect sólo se borra si puede entregarse o dejó de ser necesario. |
| RunStaleAttempt | disconnectEpoch y guard de entrada de connectAndLogonAttempt. El estado semilla representa una llamada en cola anterior al Disconnect. |
| DeleteFirst, PublishNewRow y replay | [handler.go](../tools/fakeamps/handler.go) y [dedupe.go](../tools/fakeamps/dedupe.go): identidad del publisher, begin/finish/abort y operaciones SOW. |
| Restart | [journal.go](../tools/fakeamps/journal.go): loadFromDisk y restoreDeleteRecord recuperan watermarks PUB1/DEL1. La semilla SOW ya existe antes del delete modelado. |

## Contraejemplos y enlace con Go

| Variante defectuosa | Violación TLC | Regresión del Go real |
| --- | --- | --- |
| Permitir Publish mientras se recorre el snapshot | NoLoss | TestDeepAuditLivePublishCannotOvertakeRecovery. |
| Replay legacy sin agregar persisted | PersistedRequested | Los 18 casos de TestFollowupLegacyPublishBacklogRequestsPersistedAck. |
| Aceptar otra generación al enviar replay | CurrentGenerationOnly | La familia TestDeepAuditReplayCannotCompleteAfterConnectionChanges protege la generación; este runner no la cuenta entre sus cinco overlays Go. |
| Abrir sin reparar la cola WAL | NoAcceptedLoss | TestDeepAuditPublishWALTornTailThenAppendThenRestart y su equivalente de bookmarks. |
| Informar rechazo después de retener en WAL | RejectedCannotReplay | Tests normales de aceptación y checkpoint; mutación del modelo. |
| Hacer checkpoint de un store incierto | UncertainEvidencePreserved | Tests normales de freeze e inyección de fallos; mutación del modelo. |
| Reservar worker durante intento inicial | NoCompetingInitialWorker | TestHAInitialRecoveryFailureDoesNotStartCompetingReconnect. |
| No detener HA ante error terminal | TerminalIsQuiet | TestFollowupHATerminalConnectFailureRejectsLateDisconnect. |
| Perder pending al terminar el intento nuevo, al recibir callback ocupado o al salir el worker | NoDroppedReconnect, tres variantes | TestHACancelledOldWorkerHandsOffNewSessionDisconnect; el runner aplica el overlay de conservación de pending. |
| Worker en cola que intenta conectar sobre sesión recuperada | RecoveredSessionIsPreserved | TestHAReconnectWorkerDoesNotReplaceConcurrentSession. |
| Aceptar epoch anterior | StaleAttemptIsRejected | TestHAStaleQueuedAttemptCannotOverrideNewSession. |
| No restaurar watermark de delete | DeleteReplayIsIdempotent | TestFollowupSequencedDeleteSurvivesDiskRestart. |
| No restaurar watermark de publicación | PublishReplayIsIdempotent | TestFollowupPublisherDedupSurvivesDiskJournalReload. |

Un contraejemplo de NoLoss sin publishLock tiene diez pasos. En términos del
protocolo: se toma un snapshot con el mensaje 1; se retiene 2 fuera de esa copia;
termina el replay; se publica 3; el broker aplica 1 y 3; su ack acumulado de 3
descarta también 2. Al final A={1,2,3}, R={} y B={1,3}: falta 2. La versión
normal impide retener 2 mientras el snapshot está activo.

El [contraejemplo de delete tras reapertura](../formal/BrokerDedup-counterexample.mmd)
es el diagrama generado por statechart a partir de la traza TLC. Muestra cómo,
si se omite restaurar el watermark de A, repetir su delete elimina la fila
nueva publicada por B después del delete original. La configuración normal
conserva ese watermark y satisface ambas invariantes de idempotencia.

## Cobertura de los dieciséis grupos

| Grupos | Evidencia formal de esta pasada |
| --- | --- |
| D01, D09 | Replay y solicitud de persisted, dentro de las cotas de PublishRecovery. |
| D02, D03 | Cola WAL y clasificación de resultados con WAL; quedan fuera los caminos indicados en supuestos. |
| D06 | Ownership, callbacks y handoff HA; no prueba el límite físico de 40 ms. |
| D08, D16 | Repetición por identidad y secuencia en la reapertura normal modelada. |
| D04 | No formalizado: el modelo asume secuencias crecientes; los pisos de Hybrid se verifican con tests Go. |
| D05, D15 | No formalizados: parsers y escapes se verifican sobre el código real con tests/fuzz, no con una copia matemática del parser. |
| D07 | No formalizado: sincronización comprobada con race y revisión de locks. |
| D10, D11, D12, D13, D14 | No formalizados por estos cuatro modelos; mantienen sus regresiones normales y de race. |

No se presenta el cierre de los dieciséis grupos como una prueba matemática
completa. Esta pasada añade evidencia formal acotada a parte de siete grupos.

## Reproducción y mantenimiento

```text
python -m unittest discover -s tools/formalcheck -p '*_test.py'
python tools/formalcheck/check.py
```

El runner necesita Python, Java 11+ y el toolchain Go 1.26.10. Descarga el jar
TLC 1.7.4 desde el release oficial si falta y comprueba su SHA-256 fijado. Usa
un worker, fingerprint 0 y semilla 1. Exige cola final vacía y estados distintos
positivos; un error de sintaxis no satisface un contraejemplo esperado.
En Windows, Java y Go se ejecutan con CREATE_NO_WINDOW y handles capturados.
La adaptación instalada de statechart oculta sus procesos sin detached y
normaliza CRLF; los launchers y su configuración tienen pruebas de regresión.

La skill del proyecto es
[verify-amps-recovery](../.agents/skills/verify-amps-recovery/SKILL.md).
Los hashes impiden reutilizar sin revisión un modelo contra otro Go. Se puede
actualizar el registro con --record-sync sólo después de revisar el mapeo.
El registro conserva el commit de fuentes confirmado y sus hashes. En la
sincronización actual ese commit contiene todas las fuentes cubiertas; los
registros anteriores identificaban por separado las correcciones sin commit.
`--record-sync` siempre escribe `uncommitted_sources=true`; sólo se corrige a
`false` después de comprobar con Git que las ocho fuentes están commiteadas y
sin modificaciones. La comprobación de hashes sigue siendo independiente de
esa metadata.

El control exige el conjunto exacto de ocho fuentes: rechaza registros vacíos,
incompletos, ampliados o con hashes distintos. La prueba RED detectó nueve
aceptaciones incorrectas del registro parcial o ampliado antes de agregar esa
guarda. Las diez pruebas del runner pasan; cuatro mutaciones de sus controles
de fuentes, exit status y cantidad de estados fallan por aserciones. Son tests
del verificador, separados de las mutaciones de comportamiento del cliente.

En la revisión de `22c28b481fc150cd984a21bac8bc97f67c9d855d` a
`333efdb62a74cbf5187e7a625b7e25e025b80920`, sólo cambiaron `client.go` y
`client_parity_methods.go` entre las fuentes cubiertas. `onMessage` evita
llamar a `applyAckBookkeeping` para mensajes que no son ACK y evita
`detectAndTrackDuplicate` cuando falta el bookmark; ambos helpers ya retornaban
sin efectos en esos casos. También mueve la lectura del heartbeat a su rama.
El acceso rápido a handlers globales usa un indicador atómico y un mapa
protegido por lock, fuera del alcance de los modelos. No cambiaron las
transiciones de retención, replay, WAL o HA, por lo que no se modificaron los
modelos ni sus invariantes. El rechazo inicial del verificador identificó
deriva de hashes, no una violación del modelo. Las ocho fuentes cubiertas no
tenían modificaciones pendientes al registrar esta sincronización.

El release completo es una condición independiente. La pasada 1.1.0 terminó
en PASS el 2026-11-02 a las 23:36 UTC, en 37 min 5,7 s, con veinte rondas de
integración, límites originales y diez escenarios con el cliente C++ oficial.
La pasada anterior de las 01:39 UTC duró 34 min 23 s. El [informe de tiempos](test_suite_timings.md)
detalla las mediciones. Durante el comando se usó una solicitud temporal de Windows para
evitar suspensión por inactividad; no cambia el plan de energía ni mantiene
encendida la pantalla. El intento anterior interrumpido por suspensión,
confirmada por eventos Windows 42/1, no se cuenta como release aprobado.

La repetición sobre el commit de fuentes anterior `22c28b481fc150cd984a21bac8bc97f67c9d855d` queda en
`G:/dev/caches/tmp/amps-client-go-second-fixes/formal-v1.1.0-committed`.
Sus veintidós resultados TLC, hashes y parámetros coinciden con los gráficos
ya interpretados, y las cinco mutaciones Go fueron detectadas otra vez.

La repetición normal sobre `333efdb62a74cbf5187e7a625b7e25e025b80920` quedó en
`G:/dev/caches/tmp/amps-recovery-ws-final-20261103`: siete configuraciones
normales pasaron, quince variantes produjeron el contraejemplo esperado y las
cinco mutaciones Go fueron detectadas. La comprobación C++ independiente
también pasó y sus resultados están en
`G:/dev/caches/tmp/amps-cpp-ws-final-20261103`. Los informes y trazas de
statechart se conservan fuera del checkout en
`G:/dev/caches/tmp/amps-statecharts-ws-final-20261103`.

Al explorar más allá del contraejemplo intencional `BrokenGate`, statechart
también marca `NoDeadlock`: el estado defectuoso retiene mensajes fuera del
snapshot y llega a la última generación permitida, donde la cota impide otra
reconexión. La configuración normal pasó; esa marca adicional describe la
variante rota bajo la cota y no demuestra un deadlock del programa real.
En otras variantes rotas, statechart también encontró que enviar un replay de
una generación vieja puede producir `NoLoss` al descartar con un ACK acumulado,
y que no restaurar el watermark de delete viola `NewRowSurvivesOldDelete`.
