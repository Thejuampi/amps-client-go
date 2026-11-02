# Cliente C++ oficial contra fakeamps

Fecha: 2026-11-02. Referencia: AMPS C++ **5.3.5.14**, todavía ofrecida por la
[página oficial](https://crankuptheamps.com/develop) al iniciar este pase.
El ejecutable usa `AMPS::Client` del SDK sin modificar y enlaza su biblioteca
Windows x64 original. Python sólo compila y administra los procesos; los
clientes que ejecutan las operaciones AMPS son C++.

## Diferencias reproducidas y corregidas

| Grupo | Comportamiento anterior | Corrección y evidencia |
| --- | --- | --- |
| C01: ruteo de entregas | fakeamps enviaba `sub_id`; C++ despacha publicaciones y OOF a partir de `sids`. El primer probe recibió cero publicaciones. | Emitir `sids` conservando el campo singular. `live`, colas y `oof` pasan con el SDK real; `TestCPPDeliveryRoutingFieldsAndEscapedHeaders` comprueba el frame. |
| C02: bookmark y replay | El parser no aceptaba el campo largo `bookmark` del SDK. Además, el handler requería una opción que nuestro wrapper Go agregaba. C++ no recibía el histórico EPOCH. | Aceptar ambas grafías, reconocer el bookmark, respetar el corte explícito y excluir históricos para NOW. Los tres probes de bookmark y `TestCPPBookmarkSubscribeReplayByBookmarkField` pasan. |
| C03: topics escapados | Las entregas live/OOF escribían topics sin escape JSON. Las regresiones normales reprodujeron headers inválidos. | Escapar sin agregar asignaciones en el builder. El probe live usa comillas y barra inversa; las matrices Go cubren controles, Unicode, IDs y payload exactos. |
| C04: lease sin consumidor alternativo | Al expirar un lease, el selector dejaba el mensaje pendiente aunque el dueño anterior fuera el único consumidor elegible. El probe C++ con AutoAck deshabilitado recibía una sola entrega. | Preferir otro consumidor elegible y usar al anterior como fallback. La identidad incluye conexión e ID. Pasan ambos probes de redelivery y las regresiones deterministas de capacidad y elegibilidad. |
| C05: OOF solicitado | Un `sowAndSubscribe` C++ con OOF habilitado recibía el snapshot, pero no el evento del delete confirmado. | Registrar `oof` independientemente de delta y respetarlo en delete y salida de filtro. Pasan el probe `oof` y `TestCPPOOFOptionAndDeltaSubscriptions`, incluidos los casos deshabilitados. |

El ruteo y los campos se cotejaron con `src/amps_protocol.c`,
`src/amps_protocol_generated.h`, `include/ampsplusplus.hpp` y
`include/MessageRouter.hpp` del SDK. La selección de redelivery también se
contrastó con la
[descripción oficial de colas](https://crankuptheamps.com/docs/amps-user-guide/queues/understanding-amps-queuing).
La elección de volver al único consumidor elegible es una inferencia de la
disponibilidad y las reglas de capacidad documentadas; no se observó un servidor
AMPS real.

## Gate nativo y pruebas normales

```powershell
make cpp-integration CPP_SDK_ROOT=C:/path/to/amps-c++-client-5.3.5.14-Windows
```

El gate exige Windows x64, MSVC, Python, Go y el SDK externo. Verifica versión
y SHA-256 de `amps.lib`, compila una vez y ejecuta diez escenarios: live,
EPOCH, NOW, resume explícito, SOW, ack manual, dos variantes de redelivery,
retención/replay de publicación tras reconnect y OOF. Un proceso exitoso sin
el marcador del escenario no cuenta como PASS. Faltantes, errores y timeouts
fallan; no producen skips. Los procesos se ejecutan sin ventanas de consola.

`make release` incluye este gate. `make release-hosted` conserva su alcance
anterior y no demuestra interoperabilidad C++: este runner usa el SDK Windows.
Las pruebas Go de estas correcciones pertenecen a la suite normal y al stress
ya existente; no necesitan instalar C++.

La ejecución conjunta inicial del probe pasó en **19,57 segundos**. Las
dieciséis pruebas de su runner pasaron y detectaron tres mutaciones de sus
guardas. Las pruebas de fakeamps detectaron catorce mutaciones distintas de
ruteo/bookmarks, colas y OOF. Las seis mutaciones del checker formal se solapan
con regresiones de esos contratos; no se suman para inflar la cifra.

En el release 1.1.0 completo el gate nativo volvió a pasar los diez escenarios;
incluidos los tests del runner, tomó **20,419 segundos** dentro de un release
de **37 min 5,7 s**. El [registro de tiempos](test_suite_timings.md) conserva
los timestamps y las comprobaciones de rondas y fuentes intactas.

El benchmark enfocado del builder midió una asignación por entrega, tanto para
topics normales como escapados. No hay comparación de throughput anterior y
posterior de este cambio; el gate de performance conserva su baseline.

## Verificación formal y límites

El [informe formal](cpp_fakeamps_formal.md) registra nueve configuraciones
TLC correctas, siete variantes deliberadamente defectuosas y seis mutaciones
de Go detectadas. Sus modelos cubren replay secuencial y despacho de un lease
con elegibilidad estable; no prueban el parser completo ni todos los órdenes
concurrentes de la implementación.

El cliente C++ permite comprobar qué comandos emite y qué respuestas consume.
No sustituye al servidor como oráculo de todas las reglas de SOW, filtros,
durabilidad, fairness, TLS, compresión o administración. El caso SOW verifica
registros, grupo y finalización con tamaño de lote solicitado de dos; no mide
la distribución física de frames.

Quedan fuera de este pase la confirmación persisted de una suscripción y la
frontera concurrente entre snapshot/replay y publicaciones live. La inspección
también identificó candidatos pendientes de reproducción: claves SOW iguales
entre topics de cola distintos y un ack que llegue entre enviar una entrega y
registrar su lease. No se presentan como fallos reproducidos ni como contratos
verificados. El registro de gaps Go/C++ no certifica fidelidad completa del
broker frente al servidor.
