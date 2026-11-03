# Tiempos del release y optimización de fakeamps

## Release 1.1.0 con el cliente C++ oficial

El release local estricto de **1.1.0** pasó con exit code 0 en
**2.225,716 s: 37 min 5,7 s**. Se ejecutó el 2026-11-02 entre
22:58:58,749 y 23:36:04,465 UTC. El supervisor comprobó que las **319 fuentes
y configuraciones** examinadas permanecieran intactas durante la ejecución;
el conjunto incluye 280 archivos Go. El baseline de performance conservó el
SHA-256 registrado abajo y tanto performance como cobertura aprobaron; esta
última dio **92,1 % (8.929/9.698)**.

| Fase | Tiempo transcurrido |
| --- | ---: |
| Static scan | 24,378 s |
| Gosec | 282,680 s |
| Govulncheck | 4,929 s |
| Gitleaks | 12,215 s |
| Leak-check | 12,124 s |
| Fuzz-smoke | 55,256 s |
| Stress unitario, race, shuffle, count=20 | 250,555 s |
| Integración, race, shuffle, 20 rondas, dos workers | 1.305,027 s |
| Cobertura | 11,253 s |
| Performance | 149,954 s |
| Tests finales sin race / con race | 14,320 s / 24,210 s |
| Build | 2,224 s |
| Integración final sin race | 54,074 s |
| Paridad | 1,434 s |
| Gate nativo C++, incluidos tests de su runner | 20,419 s |

El coordinador completó exactamente 20 rondas distintas por paquete, con un
máximo de dos comandos concurrentes. En el broker, la media de duración de
paquete fue 122,280 s; mínimo 119,519 s y máximo 125,368 s. La suma de
duraciones solapadas no es tiempo de reloj. El gate C++ ejecutó sus dieciséis
tests de runner y los diez escenarios del SDK oficial con PASS.

El log es `.tmp/fix-audit/release-v1.1.0-cpp-final.log`. Los timestamps,
hashes y resumen reproducible quedan en
`G:/dev/caches/tmp/amps-client-go-second-fixes/audit-artifacts/`, con prefijo
`release-v1.1.0-cpp-final`. La validación de rondas y escenarios es parte del
script que calculó el resumen.

Frente al primer release de aproximadamente 2 h 50 min, esta ejecución tardó
unas **4,60 veces menos**. Frente a los 34 min 23 s del pase anterior hay cambios
de código, pruebas adicionales y variación entre ejecuciones; no es una
comparación aislada de throughput. El costo medido del nuevo gate C++ fue
20,419 s. El resultado anterior se conserva como evidencia histórica.

Después de ese cronómetro se repitieron la suite completa sin caché, con
cobertura, y `go test -race -count=1 ./...`; ambas aprobaron, incluidas las
integraciones del CLI. fakeamps registró 80,4 % de cobertura. También pasaron
la compatibilidad exacta con Go 1.25.13, la paridad contra los headers actuales
5.3.5.14 (261 símbolos y 53 contratos), y la compilación Linux amd64 de todos
los paquetes y de los tests de `amps`. No se ejecutaron tests en Linux. El stress complementario del CLI y del
runner también pasó con race, shuffle y 20 repeticiones: 69,836 s y 1,262 s
de duración de paquete, respectivamente; no están incluidos en los 37 min.

La integración usa fakeamps, ahora también con el cliente C++ real. El
[informe de interoperabilidad](cpp_fakeamps_interop.md) y el
[alcance formal](cpp_fakeamps_formal.md) detallan qué queda sin verificar.

## Seguimiento durante la publicación

El CI del PR de release pasó en Linux y Windows. El smoke posterior del merge
corrió con race y detectó una lectura no sincronizada de `client.connection`
en `TestConnectUsesCompressionFromURIAndClientDefaults`, en el caso
`default-disabled`, semilla `1793666088900465692`. El escritor del cliente ya
utilizaba `connectionStateLock`; la prueba leía el campo directamente.

La corrección toma un snapshot bajo ese mismo lock y exige una conexión no
nula antes de comprobar su tipo. No cambia el cliente, el broker, los límites
ni el baseline. El release estricto de 37 min corresponde a la implementación
preparada antes de esta corrección exclusiva del test; el gate remoto se repite
con la prueba corregida. El log original se conserva en
`.tmp/release-publish/main-ci-failed.log`.

## Release anterior completo después de optimizar

El release final terminó con **exit code 0** y el mensaje de aprobación. Su
cronómetro registró desde el 2026-11-02 01:04:39,721 UTC hasta las
01:39:02,930 UTC: **2.063,208 s, o 34 min 23,2 s**. La ejecución comprobó que
las 277 fuentes Go y el SHA-256 del baseline de performance permanecieron
intactos; el gate de cobertura pasó con **92,1 % (8.929/9.698)** y el gate de
performance también pasó. El SHA-256 del baseline quedó en
`64DB5A6CDEE1AD878BA73C6A2B305FC14D10C1874B93CDD3F541D0318B515539`.
Frente a los aproximadamente 10.230 s del release aprobado anterior,
el tiempo total fue aproximadamente **4,96 veces menor**, una reducción de
**79,8 %**. Esa comparación es aproximada porque el extremo final anterior
proviene de la hora de escritura del log, no de un cronómetro envolvente.

El log final es `.tmp/fix-audit/release-optimized-final.log`; los timestamps y
el resumen reproducible están en
`G:/dev/caches/tmp/amps-client-go-second-fixes/audit-artifacts/release-optimized-final-timestamps.jsonl`
y `release-optimized-final-timing-summary.json`, respectivamente. Los intervalos
siguientes se calcularon a partir de las marcas de fase del mismo comando:

| Fase | Tiempo transcurrido |
| --- | ---: |
| Static scan | 17,313 s |
| Gosec | 278,192 s |
| Govulncheck | 4,019 s |
| Gitleaks | 3,890 s |
| Leak-check | 9,193 s |
| Fuzz-smoke | 51,056 s |
| Stress unitario con race, shuffle y count=20 | 242,885 s |
| Integración con race, shuffle, 20 rondas y dos workers | 1.230,179 s |
| Coverage | 8,755 s |
| Performance | 136,322 s |
| Tests finales sin race / con race | 13,300 s / 16,718 s |
| Build | 1,693 s |
| Integración final sin race | 47,796 s |
| Paridad | 1,143 s |

En los tests finales sin y con race, Go informó `(cached)` para la mayoría
de los paquetes; fakeamps, fakeampsgate, hiddenprocess y withtoolchain se
ejecutaron de nuevo. El stress unitario con `-count=20` y las 20 rondas de
integración con `-count=1` se ejecutaron sin reutilizar resultados de test.

Los tiempos por paquete del stress unitario count=20 fueron los siguientes.
Corrieron en paralelo, por lo que no se deben sumar como tiempo de reloj:

| Paquete | Tiempo de paquete |
| --- | ---: |
| `amps` | 129,427 s |
| `amps/auth/kerberos` | 1,340 s |
| `amps/capi` | 3,072 s |
| `amps/cppcompat` | 3,367 s |
| `amps/internal/bookmark` | 1,244 s |
| `amps/internal/replay` | 1,243 s |
| `amps/internal/testutil` | 1,243 s |
| `amps/internal/wal` | 6,767 s |
| `cmd/gofer` | 23,543 s |
| `internal/ampsconfig` | 4,313 s |
| `internal/gofercli` | 9,694 s |
| `internal/hiddenprocess` | 4,458 s |
| `internal/safecast` | 1,240 s |
| `tools/coveragegate` | 1,307 s |
| `tools/fakeamps` | 231,217 s |
| `tools/fakeampsgate` | 1,735 s |
| `tools/paritycheck` | 5,540 s |
| `tools/patterncheck` | 51,047 s |
| `tools/perfgate` | 1,274 s |
| `tools/perfreport` | 1,733 s |
| `tools/publicationcheck` | 3,275 s |
| `tools/withtoolchain` | 1,513 s |

`internal/gofercli/testutil` no contiene archivos de test. La integración
final sin race informó 0,114 s para `amps` y 44,002 s para `tools/fakeamps`;
el intervalo de 47,796 s de la tabla incluye la construcción, el lanzamiento
y la limpieza de los procesos.

Se verificaron 20 rondas únicas para cada paquete y un máximo de dos comandos
concurrentes. En el broker, los 20 procesos `go test` informaron una media de
115,690 s, mínimo 114,825 s, máximo 116,710 s y suma 2.313,804 s. Como sus
ejecuciones se solaparon, esa suma **no es tiempo de reloj**. Los mensajes del
coordinador miden además la compilación y el lanzamiento del comando, por lo
que sus tiempos son mayores que los informados por el paquete.

En el gate anterior, las duraciones de paquete del broker y del cliente sumaron
9.226,392 s en ejecución secuencial, sin incluir todos los costos de
lanzamiento. Esa suma es una cota inferior del tiempo de reloj anterior; frente
a los 1.230,179 s medidos del gate nuevo, indica una mejora de al menos
aproximadamente **7,5 veces**, no una medición exacta del tiempo anterior.

## Comprobaciones posteriores

Estas comprobaciones se ejecutaron después del release y no forman parte de
sus 2.063,208 s:

- Compatibilidad con Go 1.25.13: tests y build pasaron. La mayoría de los
  paquetes de test usó la caché válida de Go; fakeamps, fakeampsgate,
  hiddenprocess y withtoolchain se ejecutaron de nuevo.
- `go test -count=1 ./... -coverprofile=...` pasó con una ejecución fresca;
  informó **80,4 %** para `tools/fakeamps` y **100,0 %** para el paquete
  `internal/hiddenprocess` compilado en Windows.
- La paridad contra la referencia C++ 5.3.5.14 pasó con 261 entradas de API,
  51 entradas de comportamiento y cero gaps abiertos.
- `go test -race -count=1 ./...` también pasó completo sobre las fuentes finales.
- El stress adicional del CLI y del coordinador pasó veinte repeticiones con
  race y shuffle, usando `-run 'Integration|Gate'`: 68,940 s y 1,204 s por
  paquete, respectivamente. La selección contempla los nombres unitarios nuevos.
- La compilación de todos los paquetes para Linux amd64 pasó; no se ejecutaron
  los tests en Linux.

La evidencia está en `.tmp/fix-audit/compat-suite-optimized.log`,
`all-tests-suite-optimized.log`, `all-race-suite-optimized.log`,
`cli-runner-stress-suite-optimized.log` y `current-cpp-parity-suite-optimized.log`.

## Release aprobado antes de optimizar

La ejecución aprobada comenzó el 2026-11-01 a las 21:28:06 UTC. La última
escritura de `.tmp/fix-audit/release-final.log` es del 2026-11-02 a las
00:18:36 UTC y contiene `Release checks passed for 1.0.0.`. La diferencia es
aproximadamente **2 h 50 min 30 s**; el extremo final usa la hora de escritura
del archivo y no un cronómetro envolvente.

Este release es una ejecución distinta de los intentos anteriores. Uno de
ellos falló después de que Windows entrara en suspensión entre las
20:07:56,789 y las 20:17:46,859 UTC (`power-suspend-evidence.log` y
`release-host-suspend.log`); otro se detuvo para corregir ventanas de consola
(`release-stopped-for-console-fix.log`). Ninguno se cuenta como release
aprobado ni se suma al tiempo anterior.

También hubo reinicios por hallazgos reales: `release-pre-range-check.log`
terminó con G115 en journal y `release-pre-ha-lifecycle.log` detectó una
carrera en el ciclo de reconexión HA. Sus duraciones y fallos pertenecen a
esas ejecuciones anteriores, no al release aprobado.

El intento optimizado posterior, `.tmp/fix-audit/release-optimized.log`, se
interrumpió tras descubrir que el segundo header SOW no escapaba correctamente
un topic con comillas, controles o barras invertidas. El scan había pasado,
pero ese release quedó incompleto y no se cuenta como aprobado. Su registro
marca 571,596 s hasta la interrupción y exit code 1; son datos de ese intento,
no parte del release anterior. El problema se reproduce y corrige por separado
de las optimizaciones de tiempo.

| Gate o parte | Evidencia de la ejecución aprobada | Límite de la medición |
| --- | --- | --- |
| Static/security | `gosec` registra actividad entre 16:28:27 y 16:33:05 hora local, unos 4 min 38 s; gitleaks informa 2,27 s. También se ejecutaron vet, staticcheck, ineffassign, errcheck, patterncheck, publicationcheck, golangci-lint y govulncheck. | Los demás comandos no registran inicio y fin individuales. El intervalo de gosec abarca sus mensajes, no necesariamente toda la invocación. |
| Leak-check | `amps` 6,321 s; las dos pruebas de watcher, 0,174 s. | Duraciones informadas por `go test`, sin separar compilación. |
| Fuzz-smoke | Header 12,338 s; bookmark 11,291 s; socket options 12,252 s; composite 11,281 s. | Son cuatro invocaciones secuenciales; cada una incluye preparación además de sus 5 s de fuzz configurados. |
| Stress unitario con race, shuffle y count=20 | Fakeamps 222,755 s; amps 129,944 s; hiddenprocess 64,078 s; patterncheck 52,034 s; cmd/gofer 23,776 s. | Los paquetes se ejecutan en paralelo: sus duraciones **no se suman** para obtener tiempo transcurrido. |
| Stress de integración con race, shuffle y 20 rondas | Broker: **9.186,646 s** sumados, media 459,332 s, mínimo 425,385 s y máximo 472,227 s. Cliente: **39,746 s** sumados, media 1,987 s. | Las rondas de este gate se ejecutaron secuencialmente. El broker representa aproximadamente el 89,8 % del transcurrido total estimado del release. |
| Coverage | `amps` 6,659 s; gate agregado **92,1 %**, 8.928/9.698 sentencias. | Los paquetes de cobertura pueden solaparse. |
| Performance | Gate aprobado con `tools/perf_baseline.json` sin cambiar; el proceso interno de benchmarks de `amps` informó **135,472 s**. | Esa cifra es del proceso `go test`, no del wrapper completo, cuyos extremos no están cronometrados en este log. |
| Tests finales sin/con race | Fakeamps 11,653/13,263 s; amps 6,495/8,830 s; cmd/gofer 5,183/6,011 s, respectivamente. | Duraciones por paquete; dentro de cada invocación se solapan. |
| Build, integración final y paridad | Integración final sin race: cliente 0,106 s, broker 89,865 s; build y paridad terminaron. | Sin cronómetro independiente para build ni paridad. |

Las 20 duraciones del broker fueron, en segundos: 448,862; 431,972;
425,385; 449,048; 468,444; 472,227; 467,934; 469,639; 468,251; 469,428;
468,260; 460,034; 463,895; 467,314; 467,252; 468,843; 463,738; 453,538;
453,155; 449,427. El log del release no activa `-v` y no permite repartir
esas duraciones entre pruebas individuales.

## Trabajo de las pruebas lentas

Cada una de las dos pruebas fresh-client ejecuta 1.000 ciclos; en cada uno
abre un cliente, registra diez suscripciones y verifica el snapshot completo.
El corpus determinista contiene 1.120 filas, con 60 campos por fila. Por
consiguiente, esas dos pruebas entregan **2.240.000 registros SOW por ronda**,
o 44,8 millones en 20 rondas, además de logon, acks, unsubscribe y churn.
Los logs de conexiones registran aproximadamente 2,90 MB de salida por
snapshot; ~5,8 GB por ronda para ambos casos es una estimación a partir de
esos contadores, no tráfico medido con un capturador de red.

Una medición histórica distinta, `.tmp/stress-fixes/red-stress.log`, del
2026-10-26, registró 25,07 s para el caso HA y 178,83 s para fresh-client. No
se usa como desglose del release actual. Las pruebas de reconexión simple y
HA tienen además su presupuesto temporal configurado de 25 s; los casos fresh
mantienen sus 1.000 ciclos completos.

## Perfil antes de optimizar

Con la máquina liberada se ejecutaron únicamente los dos casos fresh con
Go 1.26.10, Windows amd64, 20 procesadores lógicos, `-race`, seed shuffle
`1793578808292351100` y sin variables que cambien el número de ciclos.
Ambos pasaron: **183,09 s** y **178,54 s**, respectivamente; paquete
**362,836 s**. El profiler estuvo activo, por lo que estos tiempos se comparan
sólo con ejecuciones instrumentadas equivalentes.

El perfil de CPU cubre 361,63 s de reloj y 390,01 s de muestras de CPU. El
validador `reconnectStressTracker.handleSOW` acumula 80,05 s de esas muestras
(20,53 %). `encoding/json.checkValid` y `decodeState.object` acumulan 32,88 s
y 44,91 s, respectivamente; sus cifras pueden solaparse con sus llamadores y
no deben sumarse como tiempo adicional. La instrumentación de race y código
externo representa una fracción importante del resto.

El perfil de asignaciones estima 25,20 GB acumulados, no memoria residente:
`finalizeFrame` 5,85 GB, copia de `connWriter.send` 5,77 GB,
`Message.Copy` 5,68 GB, `newConnWriter` 2,92 GB y el validador 0,84 GB
acumulados. La reserva inicial del journal suma 0,27 GB en el perfil. Los
archivos de perfil están fuera del repositorio; los resúmenes están en
`.tmp/fix-audit/fakeamps-fresh-before-{profile,cpu-top,alloc-top}.log`.

## Comparación integral controlada

Se repitieron los mismos dos casos con Go 1.26.10, Windows amd64, `-race`,
`-count=1`, el mismo corpus y seed `1793578808292351100`, con perfiles de CPU
y memoria activos. Ambas ejecuciones completaron sus **1.000 ciclos por caso**
y pasaron. La comparación incluye los cambios de checker y frames SOW del
snapshot perfilado, anterior a la corrección del escape del topic:

| Medición | Antes | Después |
| --- | ---: | ---: |
| Fresh-client | 183,09 s | 31,83 s |
| Fresh-client con live churn | 178,54 s | 33,26 s |
| Paquete de los dos casos | 362,836 s | 66,277 s |
| Asignaciones acumuladas, estimadas por el perfil | 25,20 GB | 18,83 GB |

El tiempo de paquete bajó **81,7 %**, una mejora de **5,47 veces**, y las
asignaciones muestreadas bajaron aproximadamente **25,3 %**. No se trata de
una medición del release completo ni de una predicción de sus 20 rondas: los
otros casos conservan su carga y las pruebas de reconnect limitadas por tiempo
pueden completar más ciclos con el procesamiento más rápido.

El perfil posterior registra 65,09 s de reloj y 88,28 s de muestras de CPU.
El builder SOW asigna 5,91 GB, `Message.Copy` 5,67 GB y `newConnWriter`
2,93 GB; el método de encolado copiador ya no figura entre los principales
asignadores del perfil. Las cifras de memoria son acumulaciones muestreadas,
no consumo residente. Resultados y resúmenes:
`.tmp/fix-audit/fakeamps-fresh-after-{profile,cpu-top,alloc-top}.log`.
Los topics de ese corpus son ASCII simples. Estos perfiles no miden el costo
ni verifican la corrección del cambio posterior para caracteres escapados.

## Cambios y mediciones aisladas

- Se valida cada corpus una sola vez y se indexan los payloads completos. La
  ruta rápida reconoce únicamente bytes idénticos; ante cualquier otra entrada
  se ejecuta el verificador JSON anterior y se conservan sus diagnósticos. El
  oráculo compara la implementación con la anterior e incluye cambios de
  todos los campos, ausencias, tipos, rutas, JSON inválido, reformateos,
  duplicados y finalización con filas faltantes.
- El envío SOW transfiere al writer el frame ya propio que devuelve el builder,
  evitando una segunda copia. Se comprueba que el frame encolado sobrevive a
  reutilizar el buffer y modificar el payload, conserva FIFO con el ack y que
  la API del writer para memoria prestada sigue copiando.
- El constructor SOW compone la cabecera y copia el payload directamente al
  frame final. Los límites de longitud y la independencia del frame se
  verifican por separado. El benchmark aislado del constructor con 2,5 KB y
  CPU=1 pasó de una mediana de 3.918 a 3.652 ns/op con race, y de 413,1 a
  387,2 ns/op sin race (cinco muestras). En la combinación de construir y
  encolar con race, la mediana fue de 6.491 a 5.010 ns/op, con dispersión en
  la línea de base; 2 → 1 asignación y 2.704 → 2.688 B/op. El ahorro se debe
  a evitar una copia de payload y una asignación pequeña, no a eliminar una
  asignación de 2,5 KB. Los logs de esas muestras están en
  `G:/dev/caches/tmp/amps-sow-frame-{before,after}[-race].txt`.
- Un hallazgo de corrección independiente requiere escapar el topic en el
  segundo header SOW cuando contiene caracteres especiales.
  `TestSOWFrameTopicJSONRoundTrip` decodifica ambos headers para comillas,
  barras invertidas, controles y Unicode, y comprueba el límite del payload;
  `TestSOWFrameExactWireBytes` comprueba los bytes exactos. Tres mutaciones que
  omiten el escape de comilla, barra invertida o control fallan por aserción
  en `G:/dev/caches/tmp/amps-sow-topic-mutations-20261101`. Este cambio no se
  presenta como optimización de velocidad. Su ruta ASCII con payload de 2,5 KB
  pasó de una mediana de 385,4 a 389,8 ns/op sin race, y de 3.526 a 3.421
  ns/op con race; mantuvo una asignación y 2.688 B/op en ambas mediciones
  (cinco muestras, CPU=1). Los logs están en
  `G:/dev/caches/tmp/amps-sow-topic-{before,after}[-race].txt`.
- `Message.Copy` se conserva: forma parte de la ruta pública `ExecuteAsync`
  ejercitada por las pruebas, aunque el callback concreto no retenga el mensaje.

| Microbenchmark, mediana de 3 muestras | Antes | Después | Asignaciones antes/después |
| --- | ---: | ---: | ---: |
| Validación de un registro SOW, race | 129.417 ns/op | 1.884 ns/op | 8 → 0; 320 → 0 B/op |
| Validación de un registro SOW, sin race | 9.631 ns/op | 166,5 ns/op | 8 → 0; 296 → 0 B/op |
| Construir y encolar frame SOW de 2,5 KB, race | 7.413 ns/op | 5.994 ns/op | 3 → 2; 5.392 → 2.704 B/op |
| Construir y encolar frame SOW de 2,5 KB, sin race | 1.044 ns/op | 647 ns/op | 3 → 2; 5.380 → 2.692 B/op |

Las pruebas nativas de consola también introducían una espera de salida del
detector de carreras en sus procesos descendientes. En una medición aislada,
los dos probes pasaron de 1,03 y 2,07 s a 0,02 y 0,04 s. La configuración
de espera se ajustó sólo en esos procesos, preservando el detector de carreras
y las demás opciones. El paquete `internal/hiddenprocess` del stress count=20
pasó de 64,078 a 4,458 s; como se ejecutó junto con otros paquetes, no se
atribuye toda esa diferencia al tiempo de reloj del release.

El gate de integración también se adaptó para ejecutar hasta dos rondas en
procesos aislados, cada uno con su par de brokers y puertos propios. Conserva
las 20 rondas, ambos paquetes, `-race`, shuffle y las validaciones. El tiempo
observado se informa en la tabla del release final; no se infiere un factor de
mejora sólo por la cantidad de workers. Diecisiete pruebas unitarias que
tenían `Integration` en el nombre se renombraron para que `-skip Integration`
no las omitiera accidentalmente. No se eliminaron pruebas ni rondas.

En CI se consolidaron dos invocaciones de gosec en una sola invocación
bloqueante. Esta corrección es independiente de `make release`; no hay una
medición de ahorro de tiempo de CI y no se incluye en el tiempo del release.

Son mediciones de componentes, no una predicción del tiempo completo del
release. El runner formal posterior al cambio del handler, anterior a la
corrección de escape en `protocol.go`, pasó las siete
configuraciones, los quince contraejemplos y las cinco mutaciones Go
(`.tmp/fix-audit/formal-suite-optimized.log`); comprueba las propiedades y
cotas de [verificación formal](formal_verification.md), no la propiedad de
ownership de los buffers ni el escape del topic, que se prueban en Go. No se
redujeron los ciclos, filas, acks, deadlines, límites ni el baseline de
performance.
