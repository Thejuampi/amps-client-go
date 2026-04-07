package gofercli

const mainHelp = `gofer - AMPS spark-compatible command line client

Usage:
  gofer help
  gofer help <command>
  gofer <command> [options]

Commands:
  ping              Test connectivity to an AMPS instance
  publish           Publish one or more messages to a topic
  subscribe         Subscribe to a topic and stream matching messages
  sow               Query the State of the World for a topic
  sow_and_subscribe Query SOW, then continue streaming live updates
  sow_delete        Delete matching SOW records
  version           Print version information

Use "gofer help <command>" for command details.
`

const pingHelp = `gofer ping - test connectivity to an AMPS instance

Usage:
  gofer ping -server <host[:port]|uri> [options]

Options:
  -server string
        AMPS server address or full URI
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -timeout duration
       Connection and logon timeout (default 10s)
`

const publishHelp = `gofer publish - publish one or more messages to a topic

Usage:
  gofer publish -server <host[:port]|uri> -topic <topic> [options]

Options:
  -server string
        AMPS server address or full URI
  -topic string
        Destination topic
  -data string
        Publish a single inline payload
  -file string
        Read payloads from a file or ZIP archive
  -delimiter value
        Split text input on the provided delimiter byte (default newline)
  -delta
        Use delta_publish instead of publish
  -copy string
        Copy published payloads to a secondary AMPS server
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -rate value
       Maximum publish rate in messages per second
  -timeout duration
        Connection and logon timeout (default 10s)
`

const subscribeHelp = `gofer subscribe - subscribe to a topic and stream matching messages

Usage:
  gofer subscribe -server <host[:port]|uri> -topic <topic> [options]

Options:
  -server string
        AMPS server address or full URI
  -topic string
        Subscription topic
  -filter string
       Content filter expression
  -format string
       Output format string ({data}, {topic}, {bookmark}, {command}, {length}, ...)
  -copy string
       Copy received payloads to a secondary AMPS server
  -delta
        Use delta_subscribe instead of subscribe
  -ack
        Ack queue messages after output/copy completes
  -backlog int
        Queue max_backlog option value
  -n int
        Stop after receiving N payloads (0 = until interrupted)
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -timeout duration
       Connection and logon timeout (default 10s)
`

const sowHelp = `gofer sow - query the State of the World for a topic

Usage:
  gofer sow -server <host[:port]|uri> -topic <topic> [options]

Options:
  -server string
        AMPS server address or full URI
  -topic string
        SOW topic
  -filter string
        Content filter expression
  -orderby string
        order_by header value
  -topn uint
        top_n header value
  -batchsize uint
        batch_size header value
  -format string
        Output format string ({data}, {topic}, {bookmark}, {command}, {length}, ...)
  -copy string
        Copy received payloads to a secondary AMPS server
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -timeout duration
       Connection and logon timeout (default 10s)
`

const sowAndSubscribeHelp = `gofer sow_and_subscribe - query SOW, then continue streaming live updates

Usage:
  gofer sow_and_subscribe -server <host[:port]|uri> -topic <topic> [options]

Options:
  -server string
        AMPS server address or full URI
  -topic string
        Topic to query and subscribe
  -filter string
        Content filter expression
  -orderby string
        order_by header value
  -topn uint
        top_n header value
  -batchsize uint
        batch_size header value
  -format string
        Output format string ({data}, {topic}, {bookmark}, {command}, {length}, ...)
  -copy string
        Copy received payloads to a secondary AMPS server
  -delta
        Use sow_and_delta_subscribe instead of sow_and_subscribe
  -ack
        Ack queue messages after output/copy completes
  -backlog int
        Queue max_backlog option value
  -n int
        Stop after receiving N payloads (0 = until interrupted)
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -timeout duration
       Connection and logon timeout (default 10s)
`

const sowDeleteHelp = `gofer sow_delete - delete matching SOW records

Usage:
  gofer sow_delete -server <host[:port]|uri> -topic <topic> [options]

Options:
  -server string
        AMPS server address or full URI
  -topic string
        SOW topic
  -filter string
        Delete records that match this filter
  -data string
        Delete using a single inline payload
  -file string
        Delete using payloads from a file or ZIP archive
  -keys string
        Delete using a comma-separated sow_keys list
  -delimiter value
        Split text input on the provided delimiter byte (default newline)
  -type string
        Message type selector (spark-compatible alias)
  -prot string
        Message type selector (spark-compatible alias)
  -proto string
       Protocol or message type selector
  -secure value
       Connection security preference (true/false/1/0/yes/no; bare -secure implies true)
  -urischeme string
       Explicit URI scheme override
  -uriopts string
       Extra URI query options to append
  -authenticator string
       Authenticator factory name (for example com.crankuptheamps.spark.DefaultAuthenticatorFactory)
  -timeout duration
       Connection and logon timeout (default 10s)
`

func mainHelpText() string {
	return mainHelp
}

func commandHelpText(command string) string {
	switch command {
	case "ping":
		return pingHelp
	case "publish":
		return publishHelp
	case "subscribe":
		return subscribeHelp
	case "sow":
		return sowHelp
	case "sow_and_subscribe":
		return sowAndSubscribeHelp
	case "sow_delete":
		return sowDeleteHelp
	default:
		return mainHelp
	}
}
