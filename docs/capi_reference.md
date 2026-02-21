# Reference: C API Compatibility (`amps/capi`)

## Purpose

`amps/capi` is a handle-oriented compatibility package for C-family AMPS APIs (`amps.h`, `amps_ssl.h`, `amps_zlib.h`) exposed as Go call points.

Compatibility target:

- lifecycle and callback control semantics from C handles
- additive compatibility surface over `amps.Client` and `amps.Message`
- Go-safe behavior where C runtime details are not directly portable

## Core Compatibility Types

Primitive and handle types:

- `Result`, `Handle`
- `Char`, `Int32`, `Int64`, `UInt32`, `UInt64`
- `Handler`, `PredisconnectHandler`
- `ThreadCreatedCallback`, `ThreadExitCallback`, `TransportFilterFunction`, `HTTPPreflightCallback`

TLS and compression compatibility types:

- `SSLMethod`, `SSLContext`, `SSLHandle`
- `ZStream`
- `ZlibVersionFunc`
- `DeflateInit2Func`, `DeflateFunc`, `DeflateEndFunc`
- `InflateInit2Func`, `InflateFunc`, `InflateEndFunc`

## Client Handle API

Lifecycle and connection:

- `ClientCreate`, `ClientSetName`, `ClientConnect`, `ClientDisconnect`, `ClientDestroy`
- `ClientGetError`, `ClientAttemptReconnect`

Send operations:

- `ClientSend`, `ClientSendWithVersion`, `ClientSendBatch`

Callback registration:

- `ClientSetMessageHandler`
- `ClientSetPreDisconnectHandler`
- `ClientSetDisconnectHandler`

Transport and threading:

- `ClientGetTransport`, `ClientGetSocket`
- `ClientSetReadTimeout`, `ClientSetIdleTime`
- `ClientSetTransportFilterFunction`
- `ClientSetThreadCreatedCallback`, `ClientSetThreadExitCallback`
- `ClientSetHTTPPreflightCallback`
- `ClientSetBatchSend`

## Message Handle API

Handle lifecycle:

- `MessageCreate`, `MessageCopy`, `MessageDestroy`, `MessageReset`

Field operations:

- `MessageGetFieldValue`, `MessageSetFieldValue`
- `MessageAssignFieldValue`, `MessageAssignFieldValueOwnership`
- `MessageSetFieldValueNTS`, `MessageSetFieldGUID`
- `MessageGetFieldLong`, `MessageGetFieldUint64`

Payload operations:

- `MessageSetData`, `MessageAssignData`, `MessageSetDataNTS`, `MessageGetData`

## SSL Compatibility API

High-level SSL entrypoints:

- `SSLInit`, `SSLInitFromContext`
- `SSLSetVerify`, `SSLLoadVerifyLocations`
- `SSLUseCertificateFile`, `SSLUsePrivateKeyFile`
- `SSLFree`, `SSLGetError`

OpenSSL-style compatibility family:

- `SSLLibraryInit`, `SSLLoadErrorStrings`
- `ERRGetError`, `ERRErrorStringN`, `ERRClearError`
- `SSLv23ClientMethod`
- `SSLCTXNew`, `SSLCTXFree`
- `SSLNew`, `SSLSetFD`, `SSLGetErrorCode`
- `SSLConnect`, `SSLRead`, `SSLCtrl`, `SSLWrite`, `SSLShutdown`, `SSLPending`
- `SSLFreeHandle`

## Zlib Compatibility API

Initialization and status:

- `ZlibInit`, `ZlibLastError`, `ZlibIsLoaded`, `ZlibVersion`

Deflate family:

- `DeflateInit2`, `DeflateInit2_`, `Deflate`, `DeflateEnd`

Inflate family:

- `InflateInit2`, `InflateInit2_`, `Inflate`, `InflateEnd`

## Runtime Utility Compatibility

- `Now`
- `NoOpFn`
- callback bridge:
  - `SetWaitingFunction`, `InvokeWaitingFunction`
  - `SetRemoveRouteFunction`, `InvokeRemoveRouteFunction`
  - `SetCopyRouteFunction`, `InvokeCopyRouteFunction`
- thread counters:
  - `GetThreadCreateCount`
  - `GetThreadJoinCount`
  - `GetThreadDetachCount`

## Behavioral Notes

- Handle APIs are compatibility wrappers around Go-managed objects.
- Socket and thread semantics follow Go runtime/transport behavior, not POSIX thread identity guarantees.
- SSL and zlib entrypoints provide compatibility contracts for AMPS client extension wiring; they are not a drop-in OpenSSL C ABI layer.

## Validation

Run symbol-level parity check:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json
```
