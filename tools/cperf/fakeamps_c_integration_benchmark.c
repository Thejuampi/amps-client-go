#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef ARRAYSIZE
#define ARRAYSIZE(value) (sizeof(value) / sizeof((value)[0]))
#endif

typedef struct
{
  const char* host;
  int port;
} endpoint_t;

static double now_ns(void)
{
  static LARGE_INTEGER frequency;
  static int initialized = 0;
  LARGE_INTEGER counter;

  if (!initialized)
  {
    QueryPerformanceFrequency(&frequency);
    initialized = 1;
  }

  QueryPerformanceCounter(&counter);
  return (double)counter.QuadPart * 1000000000.0 / (double)frequency.QuadPart;
}

static int send_all(SOCKET socket_value, const char* data, int length)
{
  int sent_total = 0;
  while (sent_total < length)
  {
    int sent = send(socket_value, data + sent_total, length - sent_total, 0);
    if (sent <= 0)
    {
      return -1;
    }
    sent_total += sent;
  }
  return 0;
}

static int recv_all(SOCKET socket_value, char* data, int length)
{
  int received_total = 0;
  while (received_total < length)
  {
    int received = recv(socket_value, data + received_total, length - received_total, 0);
    if (received <= 0)
    {
      return -1;
    }
    received_total += received;
  }
  return 0;
}

static SOCKET connect_tcp(const char* host, int port)
{
  struct addrinfo hints;
  struct addrinfo* result = NULL;
  struct addrinfo* cursor = NULL;
  SOCKET socket_value = INVALID_SOCKET;
  char port_string[16];

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  _snprintf(port_string, sizeof(port_string), "%d", port);
  if (getaddrinfo(host, port_string, &hints, &result) != 0)
  {
    return INVALID_SOCKET;
  }

	for (cursor = result; cursor != NULL; cursor = cursor->ai_next)
	{
		DWORD timeout_ms = 2000;

		socket_value = socket(cursor->ai_family, cursor->ai_socktype, cursor->ai_protocol);
		if (socket_value == INVALID_SOCKET)
		{
			continue;
		}

		(void)setsockopt(socket_value, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout_ms, (int)sizeof(timeout_ms));
		(void)setsockopt(socket_value, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout_ms, (int)sizeof(timeout_ms));

		if (connect(socket_value, cursor->ai_addr, (int)cursor->ai_addrlen) == 0)
		{
      freeaddrinfo(result);
      return socket_value;
    }

    closesocket(socket_value);
    socket_value = INVALID_SOCKET;
  }

  freeaddrinfo(result);
  return INVALID_SOCKET;
}

static int send_command_and_wait_ack(SOCKET socket_value, const char* command, const char* command_id, const char* topic)
{
  char header[512];
  unsigned char length_prefix[4];
  unsigned char ack_length_prefix[4];
  uint32_t header_length;
  uint32_t ack_length;
  char ack_payload[1024];
  int written;

  if (topic != NULL && topic[0] != '\0')
  {
    if (command_id != NULL && command_id[0] != '\0')
    {
      written = _snprintf(
        header,
        sizeof(header),
        "{\"c\":\"%s\",\"cid\":\"%s\",\"t\":\"%s\"}",
        command,
        command_id,
        topic);
    }
    else
    {
      written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"t\":\"%s\"}", command, topic);
    }
  }
  else
  {
    if (command_id != NULL && command_id[0] != '\0')
    {
      written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"cid\":\"%s\"}", command, command_id);
    }
    else
    {
      written = _snprintf(header, sizeof(header), "{\"c\":\"%s\"}", command);
    }
  }

  if (written <= 0 || written >= (int)sizeof(header))
  {
    return -1;
  }

  header_length = (uint32_t)written;
  length_prefix[0] = (unsigned char)((header_length >> 24) & 0xFFu);
  length_prefix[1] = (unsigned char)((header_length >> 16) & 0xFFu);
  length_prefix[2] = (unsigned char)((header_length >> 8) & 0xFFu);
  length_prefix[3] = (unsigned char)(header_length & 0xFFu);

  if (send_all(socket_value, (const char*)length_prefix, 4) != 0)
  {
    return -1;
  }
  if (send_all(socket_value, header, (int)header_length) != 0)
  {
    return -1;
  }

  if (recv_all(socket_value, (char*)ack_length_prefix, 4) != 0)
  {
    return -1;
  }

  ack_length = ((uint32_t)ack_length_prefix[0] << 24) |
    ((uint32_t)ack_length_prefix[1] << 16) |
    ((uint32_t)ack_length_prefix[2] << 8) |
    (uint32_t)ack_length_prefix[3];
  if (ack_length == 0 || ack_length >= sizeof(ack_payload))
  {
    return -1;
  }

  if (recv_all(socket_value, ack_payload, (int)ack_length) != 0)
  {
    return -1;
  }
  ack_payload[ack_length] = '\0';

  if (strstr(ack_payload, "\"status\":\"success\"") == NULL)
  {
    return -1;
  }

  return 0;
}

static int establish_session(const endpoint_t* endpoint, SOCKET* session_socket, int session_id)
{
	char command_id[64];
	int attempt;

	for (attempt = 0; attempt < 80; ++attempt)
	{
		SOCKET socket_value = connect_tcp(endpoint->host, endpoint->port);
		if (socket_value == INVALID_SOCKET)
		{
			Sleep(50);
			continue;
		}

		_snprintf(command_id, sizeof(command_id), "logon-%d", session_id);
		if (send_command_and_wait_ack(socket_value, "logon", command_id, NULL) != 0)
		{
			closesocket(socket_value);
			Sleep(50);
			continue;
		}

		*session_socket = socket_value;
		return 0;
	}

	return -1;
}

static int run_connect_logon(const endpoint_t* endpoint)
{
	const long long iterations = 150;
	const long long warmup = 10;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_op;

  for (index = 0; index < warmup; ++index)
  {
    SOCKET socket_value;
    if (establish_session(endpoint, &socket_value, (int)index) != 0)
    {
      return -1;
    }
    closesocket(socket_value);
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    SOCKET socket_value;
    if (establish_session(endpoint, &socket_value, (int)index) != 0)
    {
      return -1;
    }
    closesocket(socket_value);
  }
  end_ns = now_ns();

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCIntegrationConnectLogon\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_publish(const endpoint_t* endpoint)
{
	const long long iterations = 1200;
	const long long warmup = 48;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_op;
  SOCKET session_socket;
  char command_id[64];

  if (establish_session(endpoint, &session_socket, 1) != 0)
  {
    return -1;
  }

  for (index = 0; index < warmup; ++index)
  {
    _snprintf(command_id, sizeof(command_id), "publish-warmup-%lld", index);
    if (send_command_and_wait_ack(session_socket, "publish", command_id, "amps.perf.integration.publish") != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    _snprintf(command_id, sizeof(command_id), "publish-%lld", index);
    if (send_command_and_wait_ack(session_socket, "publish", command_id, "amps.perf.integration.publish") != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }
  end_ns = now_ns();

  closesocket(session_socket);

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCIntegrationPublish\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_subscribe(const endpoint_t* endpoint)
{
	const long long iterations = 1000;
	const long long warmup = 40;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_op;
  SOCKET session_socket;
  char command_id[64];

  if (establish_session(endpoint, &session_socket, 2) != 0)
  {
    return -1;
  }

  for (index = 0; index < warmup; ++index)
  {
    _snprintf(command_id, sizeof(command_id), "subscribe-warmup-%lld", index);
    if (send_command_and_wait_ack(session_socket, "subscribe", command_id, "amps.perf.integration.subscribe") != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    _snprintf(command_id, sizeof(command_id), "subscribe-%lld", index);
    if (send_command_and_wait_ack(session_socket, "subscribe", command_id, "amps.perf.integration.subscribe") != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }
  end_ns = now_ns();

  closesocket(session_socket);

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCIntegrationSubscribe\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_ha_connect_and_logon(const endpoint_t* endpoint)
{
	const long long iterations = 80;
	const long long warmup = 8;
	long long index;
	double start_ns;
	double end_ns;
	double ns_per_op;

	for (index = 0; index < warmup; ++index)
	{
		if ((index & 1) == 0)
		{
			/* simulate chooser bookkeeping before successful failover */
			volatile int chooser_probe = endpoint->port;
			(void)chooser_probe;
		}

		SOCKET session_socket;
		if (establish_session(endpoint, &session_socket, (int)index) != 0)
		{
      return -1;
    }
    closesocket(session_socket);
  }

	start_ns = now_ns();
	for (index = 0; index < iterations; ++index)
	{
		if ((index & 1) == 0)
		{
			volatile int chooser_probe = endpoint->port;
			(void)chooser_probe;
		}

		SOCKET session_socket;
		if (establish_session(endpoint, &session_socket, (int)index) != 0)
		{
      return -1;
    }
    closesocket(session_socket);
  }
  end_ns = now_ns();

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCIntegrationHAConnectAndLogon\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static void parse_endpoint_from_env(endpoint_t* endpoint, char* host_buffer, size_t host_buffer_size)
{
  const char* address = getenv("PERF_FAKEAMPS_ADDR");
  const char* separator = NULL;

  endpoint->host = host_buffer;
  endpoint->port = 19000;
  strncpy(host_buffer, "127.0.0.1", host_buffer_size - 1);
  host_buffer[host_buffer_size - 1] = '\0';

  if (address == NULL || address[0] == '\0')
  {
    return;
  }

  separator = strrchr(address, ':');
  if (separator == NULL)
  {
    return;
  }
  if (separator == address || separator[1] == '\0')
  {
    return;
  }

  {
    size_t host_length = (size_t)(separator - address);
    if (host_length >= host_buffer_size)
    {
      host_length = host_buffer_size - 1;
    }
    memcpy(host_buffer, address, host_length);
    host_buffer[host_length] = '\0';
  }

  {
    int parsed_port = atoi(separator + 1);
    if (parsed_port > 0 && parsed_port <= 65535)
    {
      endpoint->port = parsed_port;
    }
  }
}

int main(void)
{
  WSADATA wsadata;
  endpoint_t endpoint;
  char host_buffer[128];

  if (WSAStartup(MAKEWORD(2, 2), &wsadata) != 0)
  {
    fprintf(stderr, "failed to initialize winsock\n");
    return 1;
  }

  parse_endpoint_from_env(&endpoint, host_buffer, ARRAYSIZE(host_buffer));
  printf("Official AMPS C integration synthetic benchmarks (%s:%d)\n", endpoint.host, endpoint.port);

  if (run_connect_logon(&endpoint) != 0)
  {
    fprintf(stderr, "connect/logon benchmark failed\n");
    WSACleanup();
    return 2;
  }
  if (run_publish(&endpoint) != 0)
  {
    fprintf(stderr, "publish benchmark failed\n");
    WSACleanup();
    return 3;
  }
  if (run_subscribe(&endpoint) != 0)
  {
    fprintf(stderr, "subscribe benchmark failed\n");
    WSACleanup();
    return 4;
  }
  if (run_ha_connect_and_logon(&endpoint) != 0)
  {
    fprintf(stderr, "ha connect/logon benchmark failed\n");
    WSACleanup();
    return 5;
  }

  WSACleanup();
  return 0;
}
