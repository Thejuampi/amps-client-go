# Revisión de comportamiento Go frente a C++ 5.3.5.14

Fecha: 2026-10-24. Revisión de partida de Go: `217b06ee9a9849a8d1c45e8cde42ade3c8031f18`.

**Estado posterior:** los 23 grupos R01–R23 fueron corregidos. Las 32 pruebas
originales están activas en la suite normal. Consultar el
[registro de correcciones y validación](behavior_parity_fixes.md) para el estado
actual. Las descripciones y cifras siguientes conservan la foto inicial del
checkout revisado, anterior a esas correcciones.

La sospecha está confirmada: hay diferencias de implementación con consecuencias
de pérdida, duplicación y confirmación prematura de mensajes. La presencia de los
símbolos públicos y la cobertura de líneas no detectaban estos contratos rotos.

El [inventario individual](behavior_parity_inventory.md) contiene una fila por
cada una de las **748 declaraciones exportadas** encontradas fuera de `internal`
y de los tests: funciones, métodos y adaptadores de transporte. Incluye `amps`,
`amps/capi`, `amps/cppcompat` y `amps/auth/kerberos`. Los métodos promovidos por
embedding remiten a la declaración que los implementa. Los métodos exportados
de tipos privados se identifican como adaptadores, sin presentarlos como API
construible directamente por el usuario. No se cuentan campos ni constantes como
funciones.

## Referencia actual verificada

La [descarga oficial](https://crankuptheamps.com/develop) ofrece C++ **5.3.5.14**.
Su historial oficial fecha esa versión el **2026-10-19**. El paquete descargado
confirma `VERSION_NUM 5.3.5.14` y `VERSION_COMMIT b7e7cb04` en `CMakeLists.txt`.

- [Paquete Windows utilizado](https://devnull.crankuptheamps.com/releases/amps/clients/c%2B%2B/5.3.5.14/amps-c%2B%2B-client-5.3.5.14-Windows.zip).
- SHA-256: `bef9a61f814a0d4be339ffd1f49ef1cf8aa601c8609a893d1cbc375e030b6dbd`.
- [Historial oficial](https://crankuptheamps.com/history/cpp).
- Copia local: `.tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows`.
- Comparación adicional: el árbol hermano `../amps-c++-client-5.3.5.1-Windows`.

En las referencias siguientes, `$CPP` significa la raíz del paquete 5.3.5.14.
Los números de línea C++ corresponden a `include/` sin el segundo prefijo `amps/`.
El inventario distingue inspección estática, pruebas que reproducen diferencias,
extensiones Go y emulaciones. **Inspección estática no certifica equivalencia**:
no se ejecutó una comparación diferencial de todos los métodos contra AMPS real.

## Hallazgos del cliente

### R01 — P1: el ack de cola usa otro comando y pierde opciones y batching manual

`Client.Ack`, `AckMessage`, `AckDeferredAutoAck`, `Message.Ack` y `FlushAcks`
terminan enviando `c=ack`. C++ envía `c=sow_delete` con topic y bookmark. Además,
`Message.Ack("cancel")` y `AckDeferredAutoAck(..., "cancel")` colocan `cancel` en
`sub_id`, porque lo pasan al tercer argumento de `Client.Ack`. Los acks manuales
tampoco respetan `SetAckBatchSize`: el primero se envía inmediatamente.

Consecuencia: una cola real puede no confirmar, cancelar ni agrupar como espera
la aplicación. `fakeamps` aceptaba el mismo contrato equivocado del port.

Evidencia: `amps/client_parity_methods.go:840`, `amps/message.go:1170`,
`$CPP/include/ampsplusplus.hpp:5091–5178`. Reproducciones:
`TestParityAuditQueueAckCommand`, `TestParityAuditMessageAckOptions` y
`TestParityAuditManualAcksRespectBatchSize`.

### R02 — P1: MaxDepth pierde mensajes en lugar de aplicar contrapresión

`MessageStream.SetMaxDepth(1)` seguido de dos entregas elimina la primera.
`enqueueWithDepth` extrae entradas hasta hacer espacio. C++ conserva la cola,
señala que está llena y reintenta la misma entrega mientras atiende heartbeats.
Con SOW también se pueden perder registros o delimitadores de grupo.

Evidencia: `amps/message_stream.go:623`,
`$CPP/include/ampsplusplus.hpp:9549–9630`.
Reproducción: `TestParityAuditStreamFullPreservesOldest`.

### R03 — P1: un stream confirma la cola antes de que la aplicación la consuma

El callback interno del stream copia y encola el mensaje, retorna éxito y
`Client.onMessage` ejecuta auto-ack inmediatamente. C++ marca esa entrega para
diferir el ack y lo gestiona durante el avance del consumidor. Si el proceso
termina después de encolar y antes de procesar, Go ya confirmó el mensaje.

Evidencia: `amps/message_stream.go:492`, `amps/client.go:906`,
`$CPP/include/ampsplusplus.hpp:9400–9427,9571–9579`.
Reproducción: `TestParityAuditStreamDefersAutoAck`.

### R04 — P1: el bookmark store confunde recibido, procesado y descartado

Hay cuatro diferencias independientes en `MemoryBookmarkStore`:

- Descartar individualmente el mensaje 2 sube `discardedUpTo` y marca como
  descartado al mensaje 1 todavía pendiente.
- Registrar dos veces un bookmark activo hace que `Count > 1` lo clasifique como
  descartado, aunque la aplicación nunca lo haya confirmado.
- `GetMostRecent` toma el mayor bookmark recibido; puede avanzar el punto de
  recuperación más allá de un mensaje sin procesar.
- `Log` devuelve una secuencia pero no la guarda en `Message.bookmarkSeqNo`, a
  diferencia de C++; el getter del mensaje sigue devolviendo cero.

C++ mantiene entradas activas individuales y calcula un punto seguro de
recuperación. El primer y segundo problemas pueden suprimir una entrega válida;
el tercero puede omitirla al reconectar. Revisar también los wrappers de archivo,
ring y compatibilidad; no asumir que todos heredan exactamente el mismo fallo.

Evidencia: `amps/bookmark_store.go:167,208,250,345`,
`$CPP/include/MemoryBookmarkStore.hpp:244,304,392,994`.
Reproducciones: `TestParityAuditBookmarkOutOfOrderDiscard`,
`TestParityAuditBookmarkActiveRedelivery`,
`TestParityAuditBookmarkResumeKeepsUnprocessed` y
`TestParityAuditBookmarkLogSetsMessageSequence`.

### R05 — P1: se detecta el duplicado después de entregarlo

`Client.onMessage` invoca el handler de la suscripción y recién después consulta
el bookmark store y llama al handler de duplicados. C++ consulta primero y elige
entre entregar el mensaje o avisar del duplicado. Además C++ separa la operación
por cada subscription id; Go usa el mismo mensaje durante la entrega a `sids`.

Evidencia: `amps/client.go:856–897`,
`$CPP/include/ampsplusplus.hpp:8604–8648`.
Reproducción: `TestParityAuditDiscardedBookmarkNotDelivered`.

### R06 — P1: un publish guardado devuelve error, y desconectado ni se guarda

El camino rápido de `Publish*` y `DeltaPublish*` devuelve `ConnectionError` si
falla el envío después de guardar el comando. La aplicación puede reintentar y
duplicar el mensaje que el store ya reproducirá. Si ya estaba desconectado,
retorna antes de guardar. C++ 5.3.5.2 corrigió expresamente la consistencia de los
publishes guardados ante errores de envío, **AC-1450**.

Evidencia: `amps/client.go:2254–2297`,
`$CPP/include/ampsplusplus.hpp:2446,3124–3145`.
Reproducciones: `TestParityAuditStoredPublishSendFailure` y
`TestParityAuditDisconnectedPublishIsStored`.

### R07 — P1: falta la generación de resuscripción introducida por C++

La instantánea que toma `DefaultSubscriptionManager.Resubscribe` puede incluir
una suscripción que otro hilo ya está enviando en esa misma conexión. Go intenta
volver a registrarla y obtiene `SubidInUseError`. C++ 5.3.5.2 añadió generación y
coordinación entre cliente y manager precisamente para resolver este caso,
**AC-1388/AC-1454**. Un mutex alrededor del mapa no sustituye ese contrato.

Evidencia: `amps/subscription_manager.go:496`, `amps/client.go:2641`,
`$CPP/include/MemorySubscriptionManager.hpp:478,708,902`,
`$CPP/include/HAClientImpl.hpp:283,340`.
Reproducción determinista, con barrera entre snapshot y envío:
`TestParityAuditSubscribeRecoveryGeneration`.

### R08 — P1: Logon informa éxito aunque falle la resuscripción

`postLogonRecovery` comunica el error al listener y continúa; `Logon` devuelve
`nil`, por lo que el HA client puede informar éxito y resetear el backoff sin
recuperar las suscripciones. C++ propaga la excepción de `resubscribe` y el HA
client conserva el control de reconexión.

Evidencia: `amps/client_parity_methods.go:656–742`, `amps/ha_client.go:121–135`,
`$CPP/include/ampsplusplus.hpp:8527–8546`,
`$CPP/include/HAClientImpl.hpp:337–381`.
Reproducción: `TestParityAuditResubscribeFailurePropagatesFromLogon`.

### R09 — P2: Connect rechaza opciones válidas de la referencia actual

`parseSocketOptions` acepta únicamente `mt`, `tcp_nodelay`, `tcp_sndbuf`,
`tcp_rcvbuf` y `compression`. Rechaza `tcp_connecttimeout` incorporado en C++
5.3.5.5 (**AC-1507**), y opciones preexistentes como `tcp_keepalive`,
`ip_protocol_prefer` y `http_preflight`. La corrección C++ de orden de parámetros
**AC-1541** no puede considerarse incorporada: Go rechaza la opción completa.

Evidencia: `amps/client.go:157–196`, `$CPP/src/tcp.c:915`,
`$CPP/src/tcp.c` y `tcps.c` en el diff desde 5.3.5.1.
Reproducción: `TestParityAuditConnectTimeoutURI`.

### R10 — P1: un campo desconocido puede modificar otro header

El parser Go hace algunas selecciones por longitud y primer carácter.
`{"c":"ack","cid":"correct","cat":"future"}` termina con
`CommandID == "future"`. Hay casos análogos para campos de dos, cuatro y siete
caracteres. Puede romper routing o metadata cuando el servidor agrega campos.
C++ actual compara las claves exactas e ignora las desconocidas, **AC-1446**.

Evidencia: `amps/header.go:123–240`, `amps/message.go:390`,
`$CPP/src/amps_protocol.c:449–495`.
Reproducción: `TestParityAuditUnknownHeaderDoesNotOverwriteCommandID`.

### R11 — P2: SetPublishBatching no modifica los envíos

El setter y getter sólo acceden a `publishBatchSizeBytes` y
`publishBatchTimeout`. `Client.send` nunca consulta esos valores: cada publish
sale inmediatamente. También afecta a `capi.ClientSetBatchSend`.

Evidencia: `amps/client_cpp_full_methods.go:278–305`, `amps/client.go:510–579`,
`$CPP/include/ampsplusplus.hpp:5291`, `$CPP/src/client.c`.
Reproducción: `TestParityAuditPublishBatching`.

### R12 — P2: el MessageRouter público no encuentra sus rutas normales

`AddRoute("cmd-1", ...)` registra una ruta, pero `DeliverAck` y `DeliverData`
comparan contra `msgRouter.key`, que queda vacío y no tiene setter público.
La prueba agrega y entrega usando sólo la API pública: no llega ningún ack.
`DeliverDataWithID` evita ese defecto concreto. El cliente principal utiliza
otro camino de routing; no atribuirle automáticamente este fallo del helper.

Evidencia: `amps/message_router.go:159,242,273`,
`$CPP/include/MessageRouter.hpp:360–430,498`.
Reproducción: `TestParityAuditRouterAck`.

### R13 — P2: se elimina una parte vacía de un composite

C++ escribe un prefijo de cuatro bytes con longitud cero. Go retorna antes de
escribirlo. Al incluir una parte vacía entre otras, cambian los índices de todas
las posteriores. El oracle C++ compilado devuelve `empty_part_bytes=4`.

Evidencia: `amps/composite_message_builder.go:26–51`,
`$CPP/include/CompositeMessageBuilder.hpp:125–143`.
Reproducción: `TestParityAuditEmptyCompositePart`.

### R14 — P2: NVFIX Size cuenta un byte extra por campo

`AppendStrings("id", "1")` produce cinco bytes y reporta seis.
`sizeNeeded` suma tres bytes de separadores cuando se escriben dos.
El error se acumula al agregar campos.

Evidencia: `amps/nvfix_builder.go:78`,
`$CPP/include/ampsplusplus.hpp:9073–9148`.
Reproducción: `TestParityAuditNVFIXSize`.

### R15 — P2: se pierden categorías de errores del servidor

`reasonToError` no reconoce `subid in use`, `name in use` ni
`subscription already exists`, pese a existir sus categorías en Go.
`txn replay failed` tampoco tiene clasificación específica. C++ añadió
`ReplayFailedException` en 5.3.5.2, **AC-1434**. La pérdida de categoría afecta
decisiones de recuperación, y el texto original tampoco se conserva.

Evidencia: `amps/errors.go:57–77`, `amps/message.go:1357`,
`$CPP/include/Message.hpp:1825` y `AMPSException.hpp`.
Reproducciones: `TestParityAuditErrorClassification` y
`TestParityAuditReplayFailureIsClassified`.

### R16 — P1: el memory publish store reinicia secuencias pequeñas

Go arranca en 1 y permite inicializar desde el ack del servidor. C++ fabrica una
secuencia basada en tiempo al construir un memory store para evitar reutilizar
secuencias que el servidor ya vio pero que no puede confirmar por retraso de
réplicas. Ese caso fue corregido antes de la referencia original, **AC-1358**.
En la reproducción, un ack antiguo de 42 produce secuencia 43; un servidor que
ya recibió hasta 100 la trataría como duplicada.

Evidencia: `amps/publish_store.go:90–132,310`,
`$CPP/include/MemoryPublishStore.hpp:58–75`,
`$CPP/include/BlockPublishStore.hpp:746–805`.
Reproducción: `TestParityAuditFreshMemoryPublishSequence`.

### R17 — P2: GetNameHash toma otro dato del logon ack

C++ extrae el identificador del publisher del bookmark del ack. Go usa
`client_name` y, si no es decimal, un hash local. Con bookmark `123|42|` y nombre
`behavior-audit`, los getters deberían devolver `123`; Go devuelve el nombre y
otro número. Puede afectar correlación con bookmarks y diagnósticos de HA.

Evidencia: `amps/client.go:2073–2082`,
`$CPP/include/ampsplusplus.hpp:2127–2128`.
Reproducción: `TestParityAuditLogonNameHashComesFromBookmark`.

### R18 — P1: CAPI descarta ack types de texto y confunde sequence con sub id

`commandFromMessage` intenta convertir `a` con `Atoi`, aunque el wire format usa
`processed,persisted`. También convierte `s` en subscription id y nunca llama a
`SetSequenceID`. Un raw publish puede perder el ack solicitado y la secuencia de
deduplicación. Revisar todos los campos de esa conversión, no sólo estos dos.

Evidencia: `amps/capi/capi.go:281–374`,
`$CPP/src/amps_protocol.c`, `$CPP/include/amps_generated.h`.
Reproducciones en `amps/capi`: `TestParityAuditSendPreservesTextAckTypes` y
`TestParityAuditSendPreservesSequence`.

### R19 — P2: versiones con nombre se interpretan como versión cero

Para un build del servidor llamado `development`, C++ usa `99999999` como
capacidad predeterminada. Go devuelve cero. Los checks de capacidad y
`SupportsTimestampBookmarks` pueden desactivar comportamiento válido.

Evidencia: `amps/parity_version.go:32`, `$CPP/include/util.hpp:118,1327–1335`.
Reproducción: `TestParityAuditNamedVersion`.

### R20 — P1: un error de Retry del autenticador no termina Logon

El callback retorna `AuthenticationError` pero no escribe en `doneLoggingIn`.
El hilo que llamó a `Logon` sigue esperando: con timeout termina reportando
`TimedOutError`; con timeout cero puede esperar indefinidamente mientras la
conexión siga abierta. La reproducción devuelve error desde `Retry` y observa
el timeout equivocado.

Evidencia: `amps/client.go:2112–2152,2167–2187`,
`$CPP/include/ampsplusplus.hpp` en `_logon` y su control de autenticación.
Reproducción: `TestParityAuditAuthenticatorRetryErrorTerminatesLogon`.

### R21 — P2: TopN cambia los límites cero y default

C++ envía explícitamente `top_n=0` y limpia el campo con su sentinel
`AMPS_DEFAULT_TOP_N` (`UINT_MAX`). Go borra el campo al recibir cero y transmite
el sentinel como un límite grande. Una consulta limitada a cero puede pasar a
ser ilimitada. No es una diferencia de firma.

Evidencia: `amps/command.go:1011`, `$CPP/include/ampsplusplus.hpp:857–868`.
Reproducciones: `TestParityAuditTopNZeroIsExplicit` y
`TestParityAuditTopNDefaultSentinelClearsField`.

### R22 — P2: CRC es otro algoritmo

`cppcompat.CRC.Compute` usa CRC32 IEEE. El helper C++ calcula un CRC de 64 bits.
Para `123456789`, el oracle C++ devuelve `16845390139448941002`; Go devuelve
`3421780262`. No utilizar este helper para verificar identificadores o datos
calculados por el cliente C++.

Evidencia: `amps/cppcompat/types.go:153`, `$CPP/include/ampscrc.hpp:60–75`.
Reproducción: `TestParityAuditCRCMatchesCPP`.

### R23 — P1: HybridPublishStore puede repetir secuencias al cambiar de store

Go trata dos publish stores como primary/fallback con contadores independientes.
El primer store puede asignar 1; cuando falla, el segundo también asigna 1.
`DiscardUpTo(1)` se aplica a ambos y puede eliminar dos mensajes distintos.
C++ coordina un store de memoria con su respaldo de archivo y conserva el orden
de la secuencia; no es este modelo de fallback independiente.

Evidencia: `amps/cppcompat/stores.go:185–230`,
`$CPP/include/HybridPublishStore.hpp:366–395`.
Reproducción: `TestParityAuditHybridSequencesRemainUniqueOnFallback`.

## Diferencias de alcance y observaciones estáticas

Estas observaciones tienen evidencia en código, pero no deben confundirse con
las reproducciones anteriores ni presentarse como vulnerabilidades nuevas.

- **C01 — Compatibilidad parcial:** los helpers `capi.SSL*` emulan estado y
  loopback en memoria; no operan sobre el socket TLS suministrado. `Deflate` e
  `Inflate` crean un stream nuevo por llamada e ignoran las ventanas, niveles y
  modos de flush. Son diferencias ya intencionales en comentarios de código.
  El transporte `Client.Connect("tcps://...")` sí utiliza `crypto/tls`; no se
  debe confundir con esa emulación CAPI.
- **C02 — Recovery adapters:** los adapters Go son productores/iteradores de
  strings. Los C++ recuperan y actualizan pares subscription id/bookmark y el
  adapter SOW persiste mediante un cliente. El wrapper Go no implementa ese
  comportamiento. `NewMMapStoreBuffer` es un buffer RAM sin archivo. No cuentan
  como paridad de implementación por compartir nombres.
- **C03 — Idle/read timeout CAPI:** `ClientSetIdleTime` se convierte en un
  deadline absoluto de lectura. El receive loop trata ese timeout como error
  de conexión; C++ utiliza idle para trabajos periódicos. Requiere una prueba
  diferencial de socket además de los probes de esta revisión.
- **C04 — Failover:** `DefaultServerChooser.ReportFailure` avanza también ante
  `DisconnectedError`; C++ conserva el endpoint en ese caso. `Remove` elimina
  entradas en Go y es un no-op explícito en C++. `FixedDelayStrategy` espera
  incluso para un URI nuevo; C++ sólo espera para URIs repetidos. El backoff
  exponencial Go no incluye el jitter predeterminado de C++.
- **C05 — Configuraciones sin efecto:** `SetSlowClientPolicy` sólo guarda un
  puntero; no hay lectura de sus límites. `SetCompositeMessageType` tiene cuerpo
  vacío. Son extensiones Go y no se certifican como equivalentes C++.
- **C06 — DeferredExecution:** Go ejecuta las entradas en `postLogonRecovery`;
  C++ procesa ejecuciones diferidas desde el receive loop. El momento de
  ejecución no coincide. No asumir que el callback se ejecutará durante una
  conexión ya establecida en Go.
- **C07 — Conversiones parciales:** `Client.Send` reconstruye el comando desde
  una lista parcial de campos de Message. Auditar especialmente message type,
  campos de autenticación y los setters agregados al header. `GetConnectionInfo`
  usa claves distintas y omite parte del estado C++; las estrategias custom que
  consuman ese mapa necesitan un contrato documentado propio.
- **C08 — Ownership:** Message copia payloads y expone snapshots en varias
  operaciones donde C++ tiene referencias o transferencia de ownership. El
  inventario lo identifica como adaptación Go; no se considera bug sólo por
  hacer una copia. Los formatos de stores Go tampoco son binariamente
  compatibles, diferencia ya documentada y fuera de esta revisión funcional.

## Correcciones posteriores a 5.3.5.1

| Cambio upstream | Resultado en Go |
| --- | --- |
| AC-1388 / AC-1454, generación de resuscripción | Falta; R07 lo reproduce. |
| AC-1450, publishes guardados ante error de envío | Falta; R06 lo reproduce. |
| AC-1434, error específico de replay | Falta; R15 lo reproduce. |
| AC-1446, headers desconocidos | Go tolera algunos, pero otros pisan campos: R10. |
| AC-1507, connect timeout en URI | Rechazado por Go: R09. |
| AC-1541, orden de ip_protocol_prefer | Go rechaza la opción; no hay paridad que validar todavía. |
| AC-1423, Message compartido en convenience calls | Go crea Commands por llamada y protege el scratch publish; no se halló el mismo defecto específico de C++. Esto no elimina R07. |
| AC-1462, espacio al serializar escapes | Go usa buffers dinámicos; no se halló el mismo desborde de buffer fijo. R10 es otro problema. |
| AC-1572, excepciones de BookmarkStore | Existe el contrato opcional WithErrors y pruebas previas. No demuestra equivalencia de todos los callbacks ni de panics Go. |
| AC-1545, alineación numérica en ARM | No se identificó el mismo cast de C en los parsers numéricos Go. No se ejecutó ARM. |
| AC-1537 / AC-1472 / AC-1468 / AC-1469, cleanup y lectura TLS | Client usa net/tls de Go, no ese motor C. CAPI SSL es emulación: C01. |
| AC-1585, espera del thread-created callback | Go inicia una goroutine sin ese bucle de espera de un segundo; no se halló el mismo defecto. |
| AC-1615, MemoryStoreBuffer de tamaño cero | El buffer Go crece con bytes.Buffer; no tiene la misma inicialización por bloques. |

## fakeamps: mejoras realizadas y trabajo pendiente

Cambios realizados con pruebas RED verificadas antes de implementar:

1. Parser con claves exactas: un campo futuro no pisa un campo conocido.
2. Decodificación correcta de escapes, unicode y surrogate pairs; claves
   escapadas y whitespace JSON se interpretan correctamente.
3. Separación exacta de header/payload, incluso con valores futuros anidados.
4. Rechazo de JSON malformado, incluyendo separadores omitidos, trailing comma,
   escapes inválidos y números con ceros iniciales.
5. Los comandos desconocidos devuelven failure también si solicitan stats.
6. Soporte de `sow_delete` para confirmar bookmarks de cola. Dos lotes con el
   mismo cid C++ se aplican; el ack se limita al topic y elimina los registros
   confirmados de SOW antes de liberar backlog. `cancel` conserva el registro
   y lo vuelve a encolar.
7. Fuzzing con `encoding/json` como oracle independiente y benchmark específico
   del parser para evitar medir únicamente el camino feliz.

La variante legacy `c=ack` sigue soportada por compatibilidad con el cliente Go
existente. Las pruebas de conformidad usan expresamente el formato C++;
aceptar la variante legacy no valida R01.

Prioridades siguientes para que el broker encuentre bugs del cliente:

| Prioridad | Problema observado | Criterio verificable |
| --- | --- | --- |
| P1 | Dedupe de publish sequence vive en handleConnection, se pierde al reconectar. Un test actual espera aplicar dos veces la misma secuencia bajo la misma identidad. | Mismo publisher y secuencia: un único append al journal, SOW y fanout aun tras perder el ack y reconectar; cid nuevo sin secuencia sigue siendo válido. |
| P1 | La conexión no tiene un estado estricto de logon; se aceptan comandos fuera de sesión. | Publish previo a logon y re-logon inválido deben fallar según fixtures del servidor real. |
| P1 | `evaluateFilter` devuelve true para expresiones que no reconoce. | Modo de conformidad debe distinguir expresión no soportada de filtro válido y verificar `bad filter`. |
| P1 | La carrera snapshot SOW/registro de suscripción puede perder o duplicar el límite entre histórico y vivo. | Publicar exactamente entre snapshot y alta, con barreras deterministas, y verificar ausencia de huecos/duplicados. |
| P1 | Falta inyección determinista en puntos de commit/ack/disconnect. | Desconectar antes de aplicar, después de aplicar y antes del persisted ack; retrasar processed/completed por separado; fragmentar y agrupar frames. |
| P2 | Falta una cota de longitud antes de reservar readBuf y de salida descomprimida por frame. | Límites exactos, exceso por un byte y fragmentación sin reserva desproporcionada. |
| P2 | Mucho estado global mezcla instancias y obliga a tests seriales. | Instancias independientes con estado de broker propio y reloj inyectable para leases, TTL y heartbeats. |
| P2 | Se acepta el tipo de mensaje inicial del topic incluso si otro publish trae un tipo incompatible. | Failure explícito por conflicto de tipo; conservar SOW anterior. |
| P2 | La matriz `amps_parity_gaps.md` anterior estaba desactualizada: por ejemplo ya existían leases, joins y algunas funciones de filtros. | Matriz ligada a tests concretos y capacidades implementadas, sin reclamar equivalencia total de servidor. |

## Reproducción y verificación

Suite de regresión del cliente; ahora debe aprobar sin build tag:

```powershell
go test ./amps ./amps/capi ./amps/cppcompat -run '^TestParityAudit' -count=1 -timeout=20s
```

Los tests usan el contrato esperado de C++ y reproducían los fallos originales.
Se migraron a archivos `behavior_parity_regression_test.go` y se ejecutan en
cada corrida ordinaria; ya no requieren `-tags parity_audit`.

Oracle C++ de utilities, sin servidor ni cambios al código del proveedor:

```powershell
g++ -O2 -std=c++17 -I.tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows/include tools/paritycheck/testdata/behavior_oracle.cpp -o .tmp/parity-audit/behavior-oracle.exe
& .tmp/parity-audit/behavior-oracle.exe
```

Salida: `empty_part_bytes=4` y `crc_123456789=16845390139448941002`.
Es verificación funcional; no contiene resultados de performance del proveedor.

Verificaciones del cambio a fakeamps y del inventario:

```powershell
go test ./tools/paritycheck -count=1
go test ./tools/fakeamps -run '^(TestParseAMPSHeader|TestCPPQueue|TestUnknownCommand|FuzzParseAMPSHeaderJSONParity)' -count=1
go test ./tools/fakeamps -run '^$' -fuzz '^FuzzParseAMPSHeaderJSONParity$' -fuzztime=5s -parallel=2 -args '-test.fuzzcachedir=G:\dev\repos\amps-client-go\.tmp\parity-audit\fuzz-cache'
go test ./tools/fakeamps -run '^$' -bench '^BenchmarkFakeAMPS(Header|QueueAckBatch)$' -benchmem
```

La evaluación de mutaciones apunta a decodificación de escapes, igualdad exacta
de claves, topic correcto en los acks y rechazo del comando desconocido.
Se ejecutaron cuatro mutaciones mediante overlays, sin modificar el working tree:
omitir decodificación de escapes, cambiar una clave exacta por otra, quitar el
guard de topic en el ack y desactivar la rama cancel. Las pruebas correspondientes
fallaron en los cuatro casos. El inventario tiene un test AST que detecta
declaraciones omitidas, duplicadas o eliminadas; comprueba alcance de revisión,
no equivalencia de comportamiento.

### Resultados de verificación de la revisión inicial

| Verificación | Resultado |
| --- | --- |
| `go test ./... -count=1` | PASS en todos los paquetes. |
| Race detector en parser, acks C++, comandos desconocidos e inventario | PASS. No se ejecutó la suite completa con `-race`. |
| Fuzzing del parser contra `encoding/json` | PASS: 75.368 ejecuciones con dos workers. |
| `make coverage-check` | PASS: agregado `amps/...` 91,3% (8106/8876). |
| Cobertura de `tools/fakeamps` | 80,1%; helpers modificados entre 87,5% y 100%. |
| Cuatro mutaciones reales mediante overlays | Las cuatro provocaron fallos en sus tests. |
| `make perf-check` con Go 1.25.13 | PASS tras la repetición confirmatoria de `BenchmarkStreamConcurrentDequeue`; no se cambió el baseline. |
| `make static-scan`, ejecutado por `make scan` | PASS, incluyendo vet, staticcheck, ineffassign, errcheck y los gates de patrones, publicación y golangci. |
| gosec | PASS: 108 archivos, cero issues. |
| `make secret-scan` | PASS: sin secretos detectados. |
| `make markdown-scan` | PASS: 32 documentos, cero errores. |
| `make scan` completo | FALLA en govulncheck: nueve vulnerabilidades alcanzables de la biblioteca estándar Go 1.25.13 fijada por `VULN_GO_TOOLCHAIN`. |
| Probes con `-tags parity_audit` | 32 tests fallan como se esperaba; son contratos todavía pendientes, no una suite aprobada. |
| Parity checker sobre los headers 5.3.5.14 | 261 mappings, cero símbolos C++ o Go faltantes, 33 entradas de comportamiento y 25 abiertas; exit code 1 esperado. |
| Oracle C++ compilado | Confirma los dos valores indicados arriba para composite vacío y CRC. |

El resultado de govulncheck corresponde a `GO-2026-6809`, `GO-2026-6617`,
`GO-2026-6613`, `GO-2026-6612`, `GO-2026-6611`, `GO-2026-6608`,
`GO-2026-6607`, `GO-2026-6604` y `GO-2026-6603`. No se modificaron el
toolchain fijado, dependencias ni política de release. Este resultado impide
afirmar que el conjunto de gates de release esté aprobado.

El benchmark específico de fakeamps se ejecutó en Windows/amd64, Go 1.26.0,
i7-12700K, con cinco muestras de 500 ms. El header simple midió 123–127 ns/op,
24 B/op y 5 allocs/op; el escapado, 554–605 ns/op, 488 B/op y 14 allocs/op.
La validación estricta agrega trabajo al camino simple y la decodificación real
de escapes tiene un coste que el parser anterior evitaba al devolver valores
incorrectos. No se presenta como mejora de velocidad. El ack de 64 bookmarks
entre 1024 leases midió 21,6–23,6 µs/lote, 6760 B/op y 9 allocs/op; incluye
reinsertar las 64 leases para preparar el siguiente lote. La selección usa un
set para evitar recorrer cada bookmark por cada lease. Estas mediciones son
orientativas, con otros checks activos en el host; el gate de performance del
cliente se ejecutó por separado con el toolchain de release.

El estado inicial anterior fue sustituido por el
[registro de correcciones](behavior_parity_fixes.md): los 23 grupos fueron
resueltos y el gate registra `OPEN_GAPS=0`. Los demás controles de release y las
limitaciones estáticas siguen teniendo que evaluarse por separado.
