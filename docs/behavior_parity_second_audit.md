# Segunda revisión de comportamiento y preparación del release

Fecha: **2026-11-01**. Código revisado: **91dd4a8224dcdc30cd866b24a595fdbb988b2e5b**
(`release: v1.0.0`). Entorno de reproducción: Windows amd64, Go **1.26.10**.

**Resultado histórico sobre el commit base: release bloqueado.** Esta pasada reproduce **16 grupos de defectos**
mediante **19 funciones de prueba**. Ocho grupos tienen prioridad P1 y ocho P2.
El caso más grave pierde dos publicaciones retenidas mientras todas las llamadas
del cliente informan éxito. También hay recuperación de WAL que falla después
de una segunda apertura, una aceptación persistida que devuelve error y una
carrera de datos durante logon. Las prioridades expresan urgencia de corrección,
no una clasificación de vulnerabilidades de seguridad.

El trabajo posterior, las regresiones promovidas y los gates de cierre se
registran en [las correcciones de esta pasada](behavior_parity_second_fixes.md).
Las cifras RED y las referencias de código de este informe corresponden al
commit base, no al estado de la implementación corregida.

Las correcciones R01–R23 siguen teniendo sus regresiones anteriores en la suite
normal. Los escenarios de esta pasada amplían sus contratos y encuentran otros
errores; no se infiere que las 23 correcciones hayan sido inútiles. D08 era una
limitación de fakeamps ya mencionada en la primera auditoría y ahora tiene una
reproducción específica.

## Referencia y alcance de la evidencia

La [descarga oficial](https://crankuptheamps.com/develop) y el
[historial de C++](https://crankuptheamps.com/history/cpp) siguen identificando
**5.3.5.14**, publicada el **2026-10-19**. La copia local identifica el commit
`b7e7cb04`; se usa el mismo paquete Windows de la primera auditoría, cuyo SHA-256
registrado es `bef9a61f814a0d4be339ffd1f49ef1cf8aa601c8609a893d1cbc375e030b6dbd`.
En las referencias de abajo, `$CPP` es
`.tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows`; las líneas apuntan
a `include/`, sin el segundo prefijo `amps/`.

El foco fue la composición de operaciones: retener y enviar, recuperar y volver
a escribir, recuperar y publicar, recuperar y cancelar, fallar un ack y volver
a avanzar. Se inspeccionaron los caminos compartidos por las APIs públicas de
publicación, stores, recuperación, suscripciones, streams, metadatos y builders,
además del protocolo de fakeamps y las afirmaciones de sus tests. El
[inventario de 748 declaraciones](behavior_parity_inventory.md) sigue siendo un
mapa de cobertura de revisión; esta pasada no convierte cada fila en una prueba
diferencial ejecutada.

La evidencia tiene tres niveles:

- **Ejecutado en Go:** pruebas blancas con barreras, archivos temporales y TCP
  local. La integración de D01 usa el cliente público y fakeamps.
- **Comparado con C++:** inspección de la versión actual. Para D05 también se
  compiló, sin modificarla, la función `get_string` extraída del parser C del
  SDK. Pasó los casos de barra invertida, comilla y salto de línea.
- **Contrato del servidor:** documentación oficial para identidad del publisher,
  secuencias y acks. No se ejecutó un servidor AMPS real ni el cliente C++ completo.

## Resultado de las comprobaciones

| Comprobación | Resultado observado | Qué demuestra |
| --- | --- | --- |
| `go test ./... -count=1` sobre el release | PASS en todos los paquetes | Los tests existentes no detectan estos escenarios. |
| `make scan`, incluido `static-scan` | PASS; gosec 0 issues, gitleaks sin leaks | No detecta los errores funcionales reproducidos. Govulncheck distingue alcance de símbolos y módulos, detallado debajo. |
| `make coverage-check` | PASS, **91,7%** agregado, 8428/9194 statements | Cobertura alta no implica contratos completos. |
| Parity checker antes de registrar esta pasada | 261 símbolos, 0 faltantes, 35 entradas de comportamiento, `OPEN_GAPS=0` | El checker valida presencia y estados declarados, no equivalencia ejecutada. |
| 19 pruebas nuevas con `-race -cpu 1,2,8` | Las 19 fallan al menos una vez; 18 tienen fallos lógicos y una detecta data races | Los defectos están reproducidos, no son sólo hipótesis de lectura. |
| Cinco pruebas críticas, `-count 20 -cpu 1,2,8 -race` | 300 ejecuciones, 300 fallos con la aserción esperada | Orden de replay, integración con pérdida, ambos WAL y cancelación son reproducibles bajo tres configuraciones de concurrencia. |
| Función de strings del SDK compilada con GCC | 3/3 PASS | C++ decodifica los tres escapes contrastados. No certifica todo Unicode. |
| Parity checker con los hallazgos registrados | 51 entradas, `OPEN_GAPS=19`, salida 1 | 16 defectos específicos y 3 contratos generales abiertos bloquean el gate. |

La ejecución de metadatos depende de `-race`: en una repetición sin un nuevo
informe del detector puede aparecer PASS. No debe usarse ese PASS aislado para
cerrar D07. La repetición de los otros cinco casos usa barreras que fuerzan la
intercalación; no estima una probabilidad de pérdida en producción.

Los registros locales están en `.tmp/deep-audit/`: `baseline-suite.log`,
`baseline-parity.log`, `red-persisted.log`, `red-repeat.log` y `scan.log`.
No se refrescó el baseline de performance. Su SHA-256 permanece
`64db5a6cdee1ad878ba73c6a2b305fc14d10c1874b93cdd3f541d0318b515539`.
Esta revisión no constituye una nueva ejecución completa de todos los gates de
release; esos gates deben repetirse después de las correcciones.

## Hallazgos P1

### D01 Publicar durante replay puede perder mensajes retenidos

**Disparador:** hay publicaciones pendientes y otra goroutine publica mientras
`Logon` ejecuta su recuperación. `postLogonRecovery` toma `client.lock` por cada
mensaje, y el publicador nuevo puede adquirirlo antes del primer replay.

**Observado:** con secuencias pendientes 1 y 2, el socket recibe **3, 1, 2**.
La integración TCP separada retiene dos mensajes, completa logon y publica otro:
el journal de fakeamps queda con **1** mensaje y el store con **0** pendientes.
Todas las llamadas de publicación/logon devuelven éxito. El ack de la secuencia
alta limpia el backlog antes de que éste se haya enviado correctamente.

El contrato oficial indica que un ack persisted puede abarcar esa secuencia y
todas las anteriores del publisher. Por eso enviar la secuencia alta primero
invalida la recuperación segura.
[Referencia de publish](https://crankuptheamps.com/docs/amps-command-reference/commands-to-amps/publishing/publish).

**Causa:** [replay](../amps/client_parity_methods.go#L701) y
[publicación](../amps/client.go#L2391) carecen de una única disciplina de orden
para mensajes retenidos. En `$CPP/include/ampsplusplus.hpp:2446`,
`_sendSequenced` coordina logon, última secuencia enviada y `replaySingle` para
cerrar los huecos antes de emitir una secuencia mayor.

**Pruebas:** `TestDeepAuditLivePublishCannotOvertakeRecovery` y
`TestDeepAuditIntegrationLivePublishDuringReplayLosesBacklog`.
**Cierre:** conservar orden y totalidad con publicaciones concurrentes, fallos de
envío y reconexión. Mantener el assert de tres entradas; comprobar sólo que
`Publish` devuelve nil o que el store queda vacío permitiría pasar perdiendo datos.

### D02 El WAL recupera una cola parcial pero se rompe tras volver a escribir

**Disparador:** un registro termina a mitad por una escritura interrumpida. El
WAL contiene un prefijo válido y una cola sin newline, por ejemplo `{"type":`.

**Observado:** la primera reapertura no informa error. Una nueva operación
`Store` o `DiscardMessageWithError` también termina correctamente. La siguiente
reapertura falla con `unexpected end of JSON input` tanto para publicaciones
como para bookmarks. La escritura nueva se concatena con la cola incompleta.

**Causa:** [replay del WAL](../amps/internal/wal/wal.go#L137) ignora el último
fragmento sin reparar el archivo; `Append` escribe sobre ese mismo final. Es una
inconsistencia propia del almacenamiento Go, demostrada sin depender de fakeamps.

**Pruebas:** `TestDeepAuditPublishWALTornTailThenAppendThenRestart` y
`TestDeepAuditBookmarkWALTornTailThenAppendThenRestart`, con WAL y sync activados
y checkpoint cada 1000 operaciones. **Cierre:** validar el ciclo completo
recuperar → escribir → reabrir y sus secuencias/descartes exactos. El prefijo
válido debe sobrevivir y un registro completo corrupto debe seguir dando error.
Ignorar indiscriminadamente errores JSON no arregla este contrato.

### D03 Un fallo de checkpoint devuelve rechazo después de aceptar el publish

**Disparador:** el WAL admite y sincroniza la publicación, pero falla el reemplazo
del checkpoint. La reproducción deja escribible `publish.wal` y bloquea únicamente
el destino del checkpoint con un directorio vacío.

**Observado:** `Publish` devuelve el error de rename. Al retirar ese obstáculo y
reabrir el store, el mensaje sigue retenido y `LoadError` es nil. La aceptación
persistida y el error son hechos observados; la duplicación requiere que la
aplicación reintente esa publicación como una operación nueva.

**Causa:** [FilePublishStore.Store](../amps/publish_store.go#L688) retorna
`sequence, err` después de aplicar el WAL; el
[adaptador del cliente](../amps/client_parity_methods.go#L223) transforma cualquier
error en `stored=false`. Es otra ventana del contrato de aceptación tratado por
R06: la prueba anterior cubría fallo de envío después de retener.

**Prueba:** `TestDeepAuditPublishCheckpointFailureIsNotRejectedAfterRetention`.
**Cierre:** distinguir error anterior a aceptación de mantenimiento fallido tras
aceptar, o garantizar rollback duradero. No basta con descartar todos los errores
del store: un fallo real al escribir el WAL debe seguir llegando al llamador.

### D04 HybridPublishStore vuelve a secuencias pequeñas con un ack antiguo

**Disparador:** un hybrid nuevo tiene dos memory stores nuevos y recibe
`SetInitialSequence(42)` durante logon.

**Observado:** su primera publicación recibe **43**, anulando el piso de secuencia
generado por los backends nuevos. Si el servidor ya vio secuencias hasta 100 pero
el ack de logon refleja una réplica atrasada en 42, las publicaciones 43–100
pueden ser descartadas como duplicadas. Esa condición del servidor es el escenario
de riesgo; la secuencia 43 está reproducida directamente.

**Causa:** [SetInitialSequence](../amps/cppcompat/stores.go#L615) actualiza
`lastSequence`; [Store](../amps/cppcompat/stores.go#L223) impone luego
`lastSequence+1` a los backends. La corrección del store nativo no alcanza a su
wrapper. `$CPP/include/MemoryPublishStore.hpp:59` explica explícitamente el caso
de réplica atrasada y reinicio del publisher que exige preservar un piso nuevo.

**Prueba:** `TestDeepAuditFreshHybridDoesNotReuseOldPublisherSequences`.
**Cierre:** comprobar stores nuevos y reabiertos, ack viejo y nuevo, fallback y
agotamiento de secuencia. No eliminar la protección de unicidad añadida por R23.

### D05 Los escapes de headers alteran las rutas y pierden entregas

**Disparador:** un topic o subscription ID contiene una barra invertida o comilla
que requiere escape JSON en el wire.

**Observado:** topic `orders\archive` llega con el escape todavía presente.
Una ruta registrada como `sub"1` no recibe el mensaje cuyo header JSON contiene
`sub\"1`: se observan **0 callbacks**, en lugar de 1. Se reproduce en formatos
que recorren fast paths y el parser general.

**Causa:** [parseHeader](../amps/message.go#L100) conserva slices de la
representación serializada sin decodificar sus escapes. El parser de referencia,
`$CPP/src/amps_protocol.c:243`, sí decodifica las barras, comillas y controles
contrastados por el ejecutable auxiliar. Este resultado no permite afirmar
equivalencia de ambos parsers para todos los escapes Unicode.

**Pruebas:** `TestDeepAuditReceivedHeaderUnescapesText` y
`TestDeepAuditEscapedSubscriptionIDRoutes`.
**Cierre:** unificar el valor lógico de los campos entre caminos rápidos y lentos;
mantener el camino sin escapes sin copias innecesarias y pasar el perf gate
original. Corregir también el test existente que exige escapes sin decodificar.

### D06 El timeout de HA no limita la espera de logon

**Disparador:** el servidor acepta TCP y lee datos, pero nunca confirma logon.
El cliente usa `HAClient.SetTimeout(40*time.Millisecond)`.

**Observado:** `ConnectAndLogon` sigue bloqueado después de **300 ms** y sólo
termina al desconectarlo explícitamente desde la prueba. El margen amplio
observa una ausencia de deadline, no exige exactitud de reloj de 40 ms.

**Causa:** `connectAndLogonOnceWithContext` en
[ha_client.go](../amps/ha_client.go) pasa el contexto al connect y después llama
a `Logon()` sin timeout. El timeout específico de `LogonParams`, probado antes,
no cubre este uso de HA. `$CPP/include/HAClientImpl.hpp:287` pasa `_timeout`
explícitamente a `_logon`.

**Prueba:** `TestDeepAuditHATimeoutIncludesLogon`.
**Cierre:** aplicar el presupuesto a toda la tentativa, incluyendo logon y
cancelación, con limpieza de conexiones y goroutines. Conservar pruebas de éxito
y de timeout cero para no convertir la corrección en un rechazo general.

### D07 Consultar metadatos durante logon produce data races

**Disparador:** una goroutine consulta `ServerVersion`, `GetNameHash` y
`GetNameHashValue` mientras llega el ack de logon, como puede ocurrir con un
monitor durante una reconexión HA.

**Observado:** `-race` informa conflictos de lectura/escritura sobre los tres
campos. La prueba entra por los getters públicos y el procesamiento normal del
ack, usando el helper de logon existente.

**Causa:** [ServerVersion](../amps/client.go#L1862) y los
[getters del hash](../amps/client_cpp_full_methods.go#L27) leen sin sincronizar
con las [escrituras del ack](../amps/client.go#L2201). No es una diferencia de
forma de API ni una carrera introducida por el harness sobre esos campos.

**Prueba:** `TestDeepAuditLogonMetadataConcurrentReaders`.
**Cierre:** sincronizar lecturas y publicaciones de metadatos y comprobar la
coherencia del conjunto después del logon. El probe también exige los valores
finales para evitar que una corrección que deje de actualizarlos pase.

### D08 fakeamps admite un replay duplicado después de reconectar

**Disparador:** el mismo nombre de cliente, usuario y secuencia 10 publica en dos
conexiones consecutivas, conservando el mismo proceso de fakeamps.

**Observado:** el journal contiene **2** aplicaciones del mensaje; el contrato
del publisher requiere conservar la deduplicación por identidad y secuencia.
AMPS define esa identidad por nombre de cliente y usuario autenticado y la
mantiene entre conexiones.
[Identidad del publisher](https://crankuptheamps.com/faq/application-development-with-amps-clients/unique_client_naming_and_amps).

**Causa:** [handleConnection](../tools/fakeamps/handler.go#L162) crea el tracker
de secuencias por conexión. La prueba existente
`TestPublishReconnectReusesIdentityWithoutSuppressingValidCommands` llega a
exigir dos aplicaciones de una misma identidad/secuencia.

**Prueba:** `TestDeepAuditFakePublisherDedupSurvivesReconnect`.
**Cierre:** mantener el estado de publisher en el broker con el alcance y
persistencia adecuados. Probar usuarios distintos, nombres distintos y
reconexión del mismo publisher. Esta limitación era conocida; lo nuevo es su
reproducción y su registro como bloqueante de confiabilidad del harness.

## Hallazgos P2

### D09 El replay no conserva la solicitud de ack persisted

**Disparador:** `Publish`, `DeltaPublish` o `ExecuteAsync(publish)` se retienen en
un publish store para enviarse después.

**Observado:** el snapshot de replay conserva ack **0** para publish/delta y sólo
**4**, processed, para Execute. Falla en memory y file stores, seis combinaciones.
**Causa:** [storePublishCommand](../amps/client_parity_methods.go#L223) copia en
el store antes de agregar persisted al comando original. C++ establece el ack
antes de almacenar en `ampsplusplus.hpp:3135`, `:3526` y `:4903`.

Sin tráfico posterior que produzca otro ack acumulativo, la recuperación puede
dejar pendientes que no drenan y `Flush` esperando. Los acks persisted no se
solicitan por defecto en el protocolo.
[Referencia de publish](https://crankuptheamps.com/docs/amps-command-reference/commands-to-amps/publishing/publish).
No se afirma que eso por sí solo duplique la entrega en un servidor que deduplica.

**Prueba:** `TestDeepAuditReplayPreservesPersistedAck`.
**Cierre:** incluir persisted antes del snapshot sin perder los acks pedidos
por el usuario, y verificar que un replay sin nuevas publicaciones drena el store.

### D10 Unsubscribe puede ser deshecho por una recuperación antigua

**Disparador:** recovery toma un snapshot de suscripciones y se detiene al buscar
el bookmark. La aplicación completa `Unsubscribe`; luego recovery continúa.

**Observado:** en el wire aparece `unsubscribe` seguido de `subscribe` del mismo
ID. La ruta vuelve a registrarse. **Causa:**
[Resubscribe](../amps/subscription_manager.go#L522) no invalida ese snapshot ante
la cancelación; la generación de conexión no representa una baja del usuario.
En C++, `MemorySubscriptionManager.hpp:639` coordina la baja con `_resubscribing`.

**Prueba:** `TestDeepAuditUnsubscribeDuringRecoveryIsNotResurrected`.
**Cierre:** ordenar cancelación y recovery o invalidar el trabajo viejo. La
prueba admite que Unsubscribe espere a recovery; exige que el estado final y
el último comando del wire respeten la baja. Sólo verificar ausencia de carreras
de memoria no detecta este error de orden lógico.

### D11 HasNext olvida un ack fallido y permite avanzar en la siguiente llamada

**Disparador:** un stream consumió un mensaje de cola; al intentar avanzar no hay
conexión para enviar su ack.

**Observado:** el primer `HasNext` devuelve false, pero el segundo avanza y retira
el siguiente mensaje. **Causa:** [HasNext](../amps/message_stream.go#L215) pone
`previous=nil` antes de saber si `maybeAutoAck` terminó correctamente. El ack del
mensaje anterior deja de tener ese estado de retry; una reconexión puede
redeliverar trabajo ya procesado.

**Prueba:** `TestDeepAuditStreamAckFailureCannotBeBypassedByNextCall`.
**Cierre:** conservar o transferir explícitamente la responsabilidad del ack
hasta que exista un resultado válido; probar una segunda llamada y recuperación
posterior. El test anterior sólo comprobaba la primera llamada fallida.

### D12 ExecuteAsync desconectado pierde el callback de acknowledgements

**Disparador:** se acepta un publish mediante `ExecuteAsync` con publish store,
sin conexión y con handler y acks explícitos processed/persisted.

**Observado:** el mensaje se retiene y replay funciona, pero al entregar el ack
processed con su command ID el callback se ejecuta **0** veces.
**Causa:** el [retorno temprano sin conexión](../amps/client.go#L2668) omite el
registro del handler. C++ registra el handler antes del store en
`ampsplusplus.hpp:4893`. Este caso solicita ambos bits explícitamente para que
su resultado no dependa de D09.

**Prueba:** `TestDeepAuditOfflineExecutePreservesAckHandler`.
**Cierre:** mantener la correlación y el filtro de acks del usuario, sin retener
rutas después de finalizar ni disparar callbacks más veces de las debidas.

### D13 El error del handler de cola no cancela el lease

**Disparador:** AutoAck está habilitado, el mensaje tiene lease y su handler
devuelve un error de procesamiento.

**Observado:** el error se propaga, pero no sale ningún ack con `opts=cancel`.
**Causa:** [onMessage](../amps/client.go#L896) sólo llama al auto-ack cuando el
handler termina correctamente. C++ captura la excepción del handler y cancela
el lease salvo `ignoreAutoAck`, en `ampsplusplus.hpp:8643`.

El mensaje puede quedar bloqueando capacidad hasta vencer su lease. No se
demostró pérdida permanente por este caso.
**Prueba:** `TestDeepAuditFailedQueueHandlerCancelsLease`.
**Cierre:** cancelar con topic y bookmark correctos, conservar el error original
y manejar el fallo del propio envío de cancelación. Cubrir `ignoreAutoAck`.

### D14 Message.Ack envía otra confirmación con AutoAck habilitado

**Disparador:** la aplicación llama a `Message.Ack()` en un cliente con AutoAck.

**Observado:** se emite `sow_delete` inmediatamente. C++ evita el envío explícito
de `Message::ack` cuando el cliente tiene AutoAck activo
(`ampsplusplus.hpp:9811`). **Causa:** [Message.Ack](../amps/message.go#L1170)
delegó siempre en el ack del cliente y perdió esa guarda.

**Prueba:** `TestDeepAuditMessageAckDefersToAutoAck`.
**Cierre:** respetar esa distinción sin desactivar los acks manuales cuando
AutoAck es false. La evidencia demuestra un ack adicional; no demuestra por sí
misma que un servidor real elimine dos mensajes por recibirlo.

### D15 JSONMessageBuilder produce JSON inválido para controles ASCII

**Disparador:** una cadena contiene un control entre U+0000 y U+001F.

**Observado:** **27 de 32** valores generan JSON que `encoding/json` rechaza:
por ejemplo `\x00`, `\a` o `\v`. Los cinco escapes JSON habituales sí pasan.
**Causa:** [Bytes](../amps/message_type_builders.go#L58) usa
`strconv.AppendQuote`, que escribe literales de Go. Es un defecto de una extensión
Go; no se lo presenta como divergencia de una API equivalente de C++.

**Prueba:** `TestDeepAuditJSONBuilderProducesValidJSONForControls`.
**Cierre:** usar escaping JSON y comprobar round trip tanto de claves como de
valores, incluyendo comillas, barras y Unicode. Conservar benchmarks de builder
y límites de performance; una comparación contra otra función que use el mismo
escaping equivocado sería un oráculo inválido.

### D16 fakeamps usa command ID como identidad de publicación

**Disparador:** dos publicaciones distintas comparten command ID, con secuencias
10 y 11, o ambas sin secuencia.

**Observado:** sólo queda **1** entrada en el journal en ambos casos.
**Causa:** [handleConnection](../tools/fakeamps/handler.go#L685) considera
duplicada una publicación si se repite el command ID **o** la secuencia.
El protocolo describe command ID para correlacionar acknowledgements y la
secuencia para identificar publicaciones de un publisher.
[Referencia de publish](https://crankuptheamps.com/docs/amps-command-reference/commands-to-amps/publishing/publish).

**Prueba:** `TestDeepAuditFakeCommandIDIsNotPublishIdentity`.
**Cierre:** separar correlación y deduplicación; conservar ambas publicaciones
distintas y seguir rechazando un replay genuino. El cliente Go suele generar IDs
nuevos, lo que hace fácil omitir este caso en pruebas habituales.

## Por qué los controles anteriores no alcanzaron

| Área | Evidencia existente insuficiente | Aserción necesaria |
| --- | --- | --- |
| Recovery de publicación | Replay aislado y Publish aislado pasan. | Secuencia global y cantidad aplicada cuando ambas operaciones se cruzan. |
| WAL | `TestReplayIgnoresUnterminatedFinalLine` prueba sólo la primera lectura. | Escritura posterior y segundo reinicio con datos exactos. |
| Streams | `TestStreamDoesNotAdvanceAfterDeferredAckFailure` llama una vez a HasNext. | Segundo intento, profundidad intacta y posterior ack exitoso. |
| Headers | `header_message_coverage_test.go:236` espera el escape crudo. | Valor JSON decodificado y entrega por el ID original. |
| fakeamps | El test de reconexión usa igual identidad y secuencia y espera dos registros. | Una sola aplicación del replay, separada de publicaciones nuevas. |
| Race | La suite no consulta esos getters mientras cambia el logon. | Lector concurrente durante actualizaciones de los tres campos. |
| Paridad | `paritycheck` cuenta símbolos y lee `status`. | Evidencia ejecutada y entradas abiertas mientras haya contratos incumplidos. |

El porcentaje de líneas cubiertas y el fuzz de ausencia de panic no prueban
decodificación correcta, orden de mensajes ni durabilidad. Las regresiones
anteriores conservan valor, pero sus assertions delimitan lo que certifican.

El scan de seguridad, repetido con `govulncheck -show verbose`, informa cero
vulnerabilidades en símbolos alcanzables y cero en paquetes importados. A nivel
de módulos, `golang.org/x/mod v0.36.0` conserva
[GO-2026-6180](https://pkg.go.dev/vuln/GO-2026-6180) y
[GO-2026-6179](https://pkg.go.dev/vuln/GO-2026-6179), corregidas en v0.40.0.
Sus paquetes afectados no se importan en el análisis ejecutado. No se presentan
como dos exploits alcanzables del cliente, ni se resume este resultado como
ausencia absoluta de advisories. La actualización de esa dependencia queda como
mantenimiento adicional a considerar en la preparación del release.

## Observaciones estáticas pendientes de reproducción

Estos dos riesgos no se incluyen en los 16 grupos reproducidos:

- **MMap en Windows:** [mmapWriteFile](../amps/mmap_io_windows.go#L18) hace
  `os.WriteFile` y rename sin sincronizar el archivo. Con `MMap.Enabled`, el
  checkpoint del publish/bookmark store puede continuar truncando el WAL. La
  ausencia de esa barrera está en el código; el efecto ante pérdida de energía
  necesita un ensayo de fallos de persistencia. Reabrir archivos normalmente no
  prueba durabilidad ante un corte.
- **HTTP preflight y cancelación:**
  [performHTTPPreflight](../amps/client.go#L2050) lee y escribe sin recibir el
  contexto de connect ni establecer un deadline. Un peer silencioso merece una
  reproducción adicional, separada del logon silencioso ya demostrado en D06.

## Reproducción y condiciones para cerrar la revisión

Durante la detección las pruebas se ejecutaron mediante overlays `.go.txt`.
Tras corregirlas se promovieron a la suite normal de
[amps](../amps/deep_audit_probe_test.go),
[cppcompat](../amps/cppcompat/deep_audit_probe_test.go) y
[fakeamps](../tools/fakeamps/deep_audit_probe_test.go). El runner actual ejecuta
esos archivos directamente y transmite el exit code real de `go test`.
La salida 1 registrada arriba corresponde a la implementación original.

```powershell
pwsh -NoProfile -File tools/paritycheck/run_deep_audit.ps1
pwsh -NoProfile -File tools/paritycheck/run_deep_audit.ps1 -Run '^TestDeepAudit(LivePublishCannotOvertakeRecovery|IntegrationLivePublishDuringReplayLosesBacklog|PublishWALTornTailThenAppendThenRestart|BookmarkWALTornTailThenAppendThenRestart|UnsubscribeDuringRecoveryIsNotResurrected)$' -Count 20
pwsh -NoProfile -File tools/paritycheck/run_deep_string_oracle.ps1
```

El segundo script requiere GCC y el SDK local. Extrae la función de referencia
al directorio temporal; el repositorio no incorpora su implementación vendorizada.
Los probes originales reutilizaban helpers del commit revisado. Las copias
diagnósticas se eliminaron al promoverlos, para mantener una sola versión de
cada regresión.

El orden de corrección propuesto al detectar los defectos fue D01–D04 junto con D09 para aceptación,
secuencia y recuperación; D08/D16 para que fakeamps no contradiga la identidad
del publisher; luego routing, deadlines, carreras y contratos de consumo.
Cada corrección debe promover sus probes a la suite normal y agregar los casos
de éxito y fallo que indiquen sus criterios de cierre. Las entradas generales
`contract-parity-gating`, `wal-checkpoint-durability` y `ha-recovery-ordering`
quedaron abiertas junto con los 16 grupos específicos. Su resolución posterior
se registra en el documento de correcciones y el manifiesto de comportamiento.

La revisión de mutaciones exige que ordenar mal 1/2/3, omitir persisted,
descartar el estado previo antes de confirmar el ack, aceptar un snapshot de
suscripción cancelado o confundir command ID con secuencia rompa assertions
concretas. Las pruebas de detección estaban en RED: esos resultados por sí solos
no demuestran que una implementación reparada pase mutación, cobertura o performance.

Después de resolver los hallazgos: ejecutar suite ordinaria, race, integración
con fakeamps, stress completo, coverage, scan, compatibilidad y performance con
los límites originales, y una validación contra AMPS real para los contratos del
servidor. Cerrar un estado del manifiesto sin esa evidencia repetiría el problema
que esta segunda pasada acaba de demostrar.
