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

#define PENDING_ROUTE_CAPACITY 2048

typedef struct
{
	char command_id[96];
	int in_use;
} pending_route_t;

static pending_route_t pending_routes[PENDING_ROUTE_CAPACITY];

static unsigned int hash_command_id(const char* command_id)
{
	unsigned int hash = 2166136261u;
	const unsigned char* cursor = (const unsigned char*)command_id;
	while (*cursor != '\0')
	{
		hash ^= (unsigned int)(*cursor);
		hash *= 16777619u;
		cursor++;
	}
	return hash;
}

static void pending_route_register(const char* command_id)
{
	unsigned int index;
	unsigned int probe;

	if (command_id == NULL || command_id[0] == '\0')
	{
		return;
	}

	index = hash_command_id(command_id) % PENDING_ROUTE_CAPACITY;
	for (probe = 0; probe < PENDING_ROUTE_CAPACITY; ++probe)
	{
		pending_route_t* slot = &pending_routes[(index + probe) % PENDING_ROUTE_CAPACITY];
		if (!slot->in_use || strcmp(slot->command_id, command_id) == 0)
		{
			strncpy(slot->command_id, command_id, sizeof(slot->command_id) - 1);
			slot->command_id[sizeof(slot->command_id) - 1] = '\0';
			slot->in_use = 1;
			return;
		}
	}
}

static int pending_route_consume(const char* command_id)
{
	unsigned int index;
	unsigned int probe;

	if (command_id == NULL || command_id[0] == '\0')
	{
		return 1;
	}

	index = hash_command_id(command_id) % PENDING_ROUTE_CAPACITY;
	for (probe = 0; probe < PENDING_ROUTE_CAPACITY; ++probe)
	{
		pending_route_t* slot = &pending_routes[(index + probe) % PENDING_ROUTE_CAPACITY];
		if (!slot->in_use)
		{
			return 0;
		}
		if (strcmp(slot->command_id, command_id) == 0)
		{
			slot->in_use = 0;
			slot->command_id[0] = '\0';
			return 1;
		}
	}

	return 0;
}

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

static int send_command(
	SOCKET socket_value,
	const char* command,
	const char* command_id,
	const char* topic,
	const char* payload,
	int request_processed_ack)
{
  char header[512];
  unsigned char length_prefix[4];
  uint32_t header_length;
  uint32_t frame_length;
  int payload_length = 0;
  int written;
	char payload_copy[512];

	if (payload != NULL)
	{
		payload_length = (int)strlen(payload);
		if (payload_length >= (int)sizeof(payload_copy))
		{
			return -1;
		}
		memcpy(payload_copy, payload, (size_t)payload_length);
		payload_copy[payload_length] = '\0';
		payload = payload_copy;
	}

  if (topic != NULL && topic[0] != '\0')
  {
    if (command_id != NULL && command_id[0] != '\0')
    {
		if (request_processed_ack)
		{
			written = _snprintf(
				header,
				sizeof(header),
				"{\"c\":\"%s\",\"cid\":\"%s\",\"t\":\"%s\",\"a\":\"processed\"}",
				command,
				command_id,
				topic);
		}
		else
		{
			written = _snprintf(
				header,
				sizeof(header),
				"{\"c\":\"%s\",\"cid\":\"%s\",\"t\":\"%s\"}",
				command,
				command_id,
				topic);
		}
    }
    else
    {
		if (request_processed_ack)
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"t\":\"%s\",\"a\":\"processed\"}", command, topic);
		}
		else
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"t\":\"%s\"}", command, topic);
		}
    }
  }
  else
  {
    if (command_id != NULL && command_id[0] != '\0')
    {
		if (request_processed_ack)
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"cid\":\"%s\",\"a\":\"processed\"}", command, command_id);
		}
		else
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"cid\":\"%s\"}", command, command_id);
		}
    }
    else
    {
		if (request_processed_ack)
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\",\"a\":\"processed\"}", command);
		}
		else
		{
			written = _snprintf(header, sizeof(header), "{\"c\":\"%s\"}", command);
		}
    }
  }

  if (written <= 0 || written >= (int)sizeof(header))
  {
    return -1;
  }

  header_length = (uint32_t)written;
  frame_length = header_length + (uint32_t)payload_length;
  length_prefix[0] = (unsigned char)((frame_length >> 24) & 0xFFu);
  length_prefix[1] = (unsigned char)((frame_length >> 16) & 0xFFu);
  length_prefix[2] = (unsigned char)((frame_length >> 8) & 0xFFu);
  length_prefix[3] = (unsigned char)(frame_length & 0xFFu);

	if (send_all(socket_value, (const char*)length_prefix, 4) != 0)
	{
		return -1;
	}
	pending_route_register(command_id);
  if (send_all(socket_value, header, (int)header_length) != 0)
  {
    return -1;
  }
	if (payload_length > 0)
	{
		if (send_all(socket_value, payload, payload_length) != 0)
		{
			return -1;
		}
	}

  return 0;
}

static int extract_json_string_field(const char* payload, const char* field_name, char* output, size_t output_length)
{
	char token[64];
	const char* start;
	const char* end;
	size_t length;

	if (payload == NULL || field_name == NULL || output == NULL || output_length == 0)
	{
		return 0;
	}

	_snprintf(token, sizeof(token), "\"%s\":\"", field_name);
	start = strstr(payload, token);
	if (start == NULL)
	{
		return 0;
	}
	start += strlen(token);
	end = strchr(start, '"');
	if (end == NULL)
	{
		return 0;
	}

	length = (size_t)(end - start);
	if (length >= output_length)
	{
		length = output_length - 1;
	}
	memcpy(output, start, length);
	output[length] = '\0';
	return 1;
}

static int read_ack(SOCKET socket_value)
{
  unsigned char ack_length_prefix[4];
  uint32_t ack_length;
  char ack_payload[1024];
	char command_id[96];
	char ack_type[96];

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
	if (extract_json_string_field(ack_payload, "a", ack_type, sizeof(ack_type)))
	{
		if (strstr(ack_type, "processed") == NULL)
		{
			return -1;
		}
	}
	if (extract_json_string_field(ack_payload, "cid", command_id, sizeof(command_id)))
	{
		if (!pending_route_consume(command_id))
		{
			return -1;
		}
	}

	return 0;
}

static int send_command_and_wait_ack(
	SOCKET socket_value,
	const char* command,
	const char* command_id,
	const char* topic,
	const char* payload,
	int request_processed_ack)
{
  if (send_command(socket_value, command, command_id, topic, payload, request_processed_ack) != 0)
  {
    return -1;
  }
  return read_ack(socket_value);
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
		if (send_command_and_wait_ack(socket_value, "logon", command_id, NULL, NULL, 0) != 0)
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
  double total_ns;
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

  total_ns = 0.0;
  for (index = 0; index < iterations; ++index)
  {
    SOCKET socket_value;
		double start_ns;
		double end_ns;

		start_ns = now_ns();
    if (establish_session(endpoint, &socket_value, (int)index) != 0)
    {
      return -1;
    }
		end_ns = now_ns();
		total_ns += (end_ns - start_ns);
    closesocket(socket_value);
  }

  ns_per_op = total_ns / (double)iterations;
  printf("OfficialCIntegrationConnectLogon\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_publish(const endpoint_t* endpoint)
{
	const long long iterations = 1200;
	const long long warmup = 48;
  long long index;
  double total_ns;
  double ns_per_op;
  SOCKET session_socket;
  char command_id[64];
	const char* payload = "{\"integration\":true}";

  if (establish_session(endpoint, &session_socket, 1) != 0)
  {
    return -1;
  }

  for (index = 0; index < warmup; ++index)
  {
    _snprintf(command_id, sizeof(command_id), "publish-warmup-%lld", index);
		if (send_command_and_wait_ack(session_socket, "publish", command_id, "amps.perf.integration.publish", payload, 1) != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }

  total_ns = 0.0;
  for (index = 0; index < iterations; ++index)
  {
    double start_ns;
    double end_ns;

		_snprintf(command_id, sizeof(command_id), "publish-%lld", index);
		start_ns = now_ns();
		if (send_command_and_wait_ack(session_socket, "publish", command_id, "amps.perf.integration.publish", payload, 1) != 0)
		{
			closesocket(session_socket);
			return -1;
		}
		end_ns = now_ns();
		total_ns += (end_ns - start_ns);
	}

  closesocket(session_socket);

  ns_per_op = total_ns / (double)iterations;
  printf("OfficialCIntegrationPublish\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_subscribe(const endpoint_t* endpoint)
{
	const long long iterations = 1000;
	const long long warmup = 40;
  long long index;
  double total_ns;
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
		if (send_command_and_wait_ack(session_socket, "subscribe", command_id, "amps.perf.integration.subscribe", NULL, 1) != 0)
    {
      closesocket(session_socket);
      return -1;
    }
  }

  total_ns = 0.0;
  for (index = 0; index < iterations; ++index)
  {
    double start_ns;
    double end_ns;

		_snprintf(command_id, sizeof(command_id), "subscribe-%lld", index);
		start_ns = now_ns();
		if (send_command_and_wait_ack(session_socket, "subscribe", command_id, "amps.perf.integration.subscribe", NULL, 1) != 0)
		{
			closesocket(session_socket);
			return -1;
		}
		end_ns = now_ns();
		total_ns += (end_ns - start_ns);

		if (send_command_and_wait_ack(session_socket, "unsubscribe", command_id, NULL, NULL, 1) != 0)
		{
			closesocket(session_socket);
			return -1;
		}
  }

  closesocket(session_socket);

  ns_per_op = total_ns / (double)iterations;
  printf("OfficialCIntegrationSubscribe\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
  return 0;
}

static int run_ha_connect_and_logon(const endpoint_t* endpoint)
{
	const long long iterations = 80;
	const long long warmup = 8;
	long long index;
	double total_ns;
	double ns_per_op;

	for (index = 0; index < warmup; ++index)
	{
		SOCKET session_socket;
		if (establish_session(endpoint, &session_socket, (int)index) != 0)
		{
      return -1;
    }
		closesocket(session_socket);
	}

	total_ns = 0.0;
	for (index = 0; index < iterations; ++index)
	{
		SOCKET session_socket;
		double start_ns;
		double end_ns;

		start_ns = now_ns();
		if (establish_session(endpoint, &session_socket, (int)index) != 0)
		{
			return -1;
		}
		end_ns = now_ns();
		total_ns += (end_ns - start_ns);
		closesocket(session_socket);
	}

	ns_per_op = total_ns / (double)iterations;
	printf("OfficialCIntegrationHAConnectAndLogon\t%lld\t%.2f ns/op\n", iterations, ns_per_op);
	return 0;
}

static void parse_endpoint_from_env(endpoint_t* endpoint, char* host_buffer, size_t host_buffer_size)
{
  const char* uri = getenv("PERF_FAKEAMPS_URI");
  const char* address = getenv("PERF_FAKEAMPS_ADDR");
  const char* candidate = address;
  const char* separator = NULL;

  endpoint->host = host_buffer;
  endpoint->port = 19000;
  strncpy(host_buffer, "127.0.0.1", host_buffer_size - 1);
  host_buffer[host_buffer_size - 1] = '\0';

  if (uri != NULL && uri[0] != '\0')
  {
    const char* host_start = strstr(uri, "://");
    const char* path_start = NULL;
    static char uri_host_port[256];
    size_t host_port_length;

    if (host_start != NULL)
    {
      host_start += 3;
      path_start = strchr(host_start, '/');
      if (path_start == NULL)
      {
        host_port_length = strlen(host_start);
      }
      else
      {
        host_port_length = (size_t)(path_start - host_start);
      }
      if (host_port_length >= sizeof(uri_host_port))
      {
        host_port_length = sizeof(uri_host_port) - 1;
      }
      memcpy(uri_host_port, host_start, host_port_length);
      uri_host_port[host_port_length] = '\0';
      candidate = uri_host_port;
    }
  }

  if (candidate == NULL || candidate[0] == '\0')
  {
    return;
  }

  separator = strrchr(candidate, ':');
  if (separator == NULL)
  {
    return;
  }
  if (separator == address || separator[1] == '\0')
  {
    return;
  }

  {
    size_t host_length = (size_t)(separator - candidate);
    if (host_length >= host_buffer_size)
    {
      host_length = host_buffer_size - 1;
    }
    memcpy(host_buffer, candidate, host_length);
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
