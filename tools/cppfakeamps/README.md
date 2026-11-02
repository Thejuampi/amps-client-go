# Official C++ client interoperability probe

This opt-in gate builds the probe with the **60East AMPS C++ 5.3.5.14** Windows
x64 SDK and its supplied release `amps.lib`, then runs it against a freshly
built local `fakeamps`. All AMPS operations are performed by the official C++
`AMPS::Client`; Python only builds and orchestrates the processes. The SDK is
not included in this repository. Obtain the Windows distribution from
[60East](https://crankuptheamps.com/develop) under its license.

Requirements are Windows x64, Python 3.9 or later, Go, and Visual Studio 2022 C++ x64
Build Tools discoverable by `vswhere`. The runner requires the unmodified
5.3.5.14 SDK release library, SHA-256
`270ec38a8f1bfd9cad58a19bf9a113deae0aecf74d6d55deb5680c070ac65836`.
It checks the version and fingerprint before compiling. Missing prerequisites,
build failures, timeouts, and probe failures are errors, never skips.

```powershell
python tools/cppfakeamps/run.py --sdk C:/path/to/amps-c++-client-5.3.5.14-Windows
```

Select individual, distinct cases using repeatable `--scenario`, for example:

```powershell
python tools/cppfakeamps/run.py --sdk C:/path/to/amps-c++-client-5.3.5.14-Windows --scenario live --scenario sow
```

The default run covers:

- `live`: logon and exactly one publish/subscribe delivery, including a topic
  containing quotes and a backslash.
- `bookmark-epoch`, `bookmark-now`, and `bookmark-resume`: ordered historical
  and live delivery, exclusion of historical messages at NOW, and resumption
  after a bookmark obtained from an actual C++ delivery.
- `sow`: three distinct keyed records with requested batch size two, the group
  markers, and stream completion. It verifies the C++ API's results, not the
  number of physical frames used by the broker.
- `queue-ack`, `queue-lease`, and `queue-single-lease`: backlog release after
  explicit acknowledgements, redelivery to a competing consumer or the only
  consumer after lease expiry, matching bookmarks, and no redelivery after ack.
- `retained-reconnect`: offline publish retention in the SDK's
  `MemoryPublishStore`, replay and persisted acknowledgement after reconnect,
  and no duplicate after another reconnect.
- `oof`: an OOF-enabled SOW subscription receives the expected delete event.

The broker uses a temporary localhost port, a 1.5-second queue lease, and a
100-millisecond lease check. Builds and copied SDK headers stay in a temporary
directory under `.tmp/cppfakeamps`, which is removed on completion. Child
processes are started without visible windows; startup and individual probe
execution have deadlines, and the broker is stopped on success or failure.
The runner's own tests do not need the SDK:

```powershell
python -m unittest discover -s tools/cppfakeamps -p 'test_*.py' -v
```

`fakeamps` models a subset of AMPS. Passing this gate is evidence of these
specific client/broker interactions, not proof of compatibility with a real
AMPS server.
