// Package amps contains the AMPS Go client that makes it easy to build reliable, 
// high performance messaging applications in Go.
//
// AMPS Go Client
//
// The AMPS Go Client lets you write blazing fast messaging applications that use the powerful AMPS technology.
//
// This is the detailed reference for the client. For an overview and introduction, see
// the Go Development Guide that came with the client download, or this repository documentation.
//
// Features
//
// - Connecting and logging on to the AMPS Server 5.2 or higher;
//
// - Authenticator interface support;
//
// - Command interface support (any command can be built and sent via Client.ExecuteAsync or Client.Execute method)
//
// - Publishing, either via convenience methods or using the Command interface;
//
// - Subscriptions, SOW queries, and other commands using the Command interface and ExecuteAsync method;
//
// - Experimental: Subscriptions, SOW queries, and other commands using synchronous MessageStream interface.
//
// Quick Start
//
// Once the client is installed, it can be used in your Go projects:
//
//     package main
//     
//     import (
//         "amps"
//         "fmt"
//     )
//     
//     func main() {
//         fmt.Println(amps.ClientVersion)
//     }
//
// You are now ready to use AMPS in your project! Here are a few Go examples to get you started:
//
// Connect and Subscribe
//
// In this example, we connect to an AMPS server running locally and initiate a subscription to the "orders" topic. 
// As new orders are posted, the message handler is invoked with each message, and prints the data of each order to the 
// console:
//
//   package main
//   
//   import (
//       "amps"
//       "fmt"
//   )
//   
//   func main() {
//       client := amps.NewClient("my-application")
//   
//       err := client.Connect("tcp://localhost:9000/amps/json")
//       if err != nil { fmt.Println(err); return }
//   
//       err = client.Logon()
//       if err != nil { fmt.Println(err); return }
//   
//       // Connected, let's subscribe to a topic now
//       subID, err := client.Subscribe(func(message *amps.Message) (msgError error) {
//           fmt.Println(string(message.Data()))
//   	return
//       }, "orders")
//   
//       // Now we can publish to the above subscription
//       err = client.Publish("orders", `{"order": "Tesla 3", "qty": 10}`)
//   }
//
// Publish a Message
//
// With AMPS, publishing is simple, as shown in this example. We connect to an AMPS server running locally, and publish 
// a single message to the messages topic. To simply publish a message, there is no need to predeclare the topic or 
// configure complex routing. Any subscription that has asked for XML messages on the messages topic will receive the 
// message. Alternatively, the PublishBytes() method is available -- it works great with the built-in Go libraries such 
// as json.
//
//     package main
//     
//     import (
//         "amps"
//         "fmt"
//     )
//     
//     func main() {
//         client := amps.NewClient("publish-example")
//         
//         err := client.Connect("tcp://localhost:9000/amps/xml")
//         if err != nil { fmt.Println(err); return } 
//         
//         err = client.Logon()
//         if err != nil { fmt.Println(err); return }
//     
//         err = client.Publish("messages", "<hi>Hello, world!</hi>")
//     }
// 
// Disconnect Handling
//
// When AMPS detects a disconnection, the reconnect function is called from DisconnectHandler to re-establish connection, 
// and our connectToNextURI method reconnects to the next URI available in the list. Notice that the DisconnectHandler 
// is only called after a successful connection has been established, otherwise a connection error is returned by the 
// Connect method of the Client.
//
//     package main
//     
//     import (
//     	"fmt"
//     	"time"
//     	"amps"
//     )
//     
//     // The currently selected URI index (0 by default)
//     var currentURI int
//     
//     // A list of URIs to connect
//     var uris = []string{
//         "tcp://localhost:9000/amps/json",
//         "tcp://localhost:9001/amps/json",
//         "tcp://localhost:9002/amps/json",
//     }
//     
//     
//     /*
//       This function is invoked by the disconnect handler,
//       with a URI to connect.
//     */
//     func connectToNextURI(client *amps.Client, uri string) {
//         fmt.Println("Connecting to", uri, "...")
//     
//         err := client.Connect(uri)
//         // Can't establish connection
//         if err != nil {
//             time.Sleep(time.Second)
//             currentURI = (currentURI + 1) % len(uris);
//             connectToNextURI(client, uris[currentURI])
//             return
//         }
//     
//         err = client.Logon()
//         // Can't logon
//         if err !=  nil {
//             fmt.Println("Failed to logon")
//             return
//         }
//     
//         // Successfully connected
//         fmt.Println("Connected!")
//     }
//     
//     
//     func main() {
//         var client = amps.NewClient("failover-demo")
//     
//         /*
//          In our disconnect handler, we invoke connectToNextURI(),
//          with the next URI in our array of URIs and display
//          a warning message. This simplistic handler never gives up,
//          but in a typical implementation, you would likely stop
//          attempting to reconnect at some point.
//         */
//         client.SetDisconnectHandler(func(cl *amps.Client, err error) {
//             fmt.Println("Switching to the next URI...")
//     
//             currentURI = (currentURI + 1) % len(uris);
//             connectToNextURI(client, uris[currentURI]);
//         })
//     
//         // We begin by connecting to the first URI
//         connectToNextURI(client, uris[currentURI]);
//     }
//
// Query the Contents of a SOW Topic
//
// State-of-the-World ("SOW") topics in AMPS combine the power of a database table with the performance of a 
// publish-subscribe system. Use the AMPS Go client to query the contents of a SOW topic:
//
//     package main
//     
//     import (
//         "amps"
//         "fmt"
//     )
//     
//     func main() {
//         client := amps.NewClient("my-application")
//     
//         err := client.Connect("tcp://localhost:9000/amps/json")
//         if err != nil { fmt.Println(err); return }
//     
//         err = client.Logon()
//         if err != nil { fmt.Println(err); return }
//     
//         _, err := client.Sow(func(message *amps.Message) (msgErr error) {
//             if command, _ := message.Command(); command == amps.CommandSOW {
//                 // Print the message data
//                 fmt.Println(string(message.Data()))
//             }
//             return
//         }, "orders", "/symbol = 'ROL'")
//     
//         if err != nil { fmt.Println("SOW Query Error:", err); return }
//     }
//
// Command Interface
//
// Even though AMPS clients provide the above named convenience methods for core AMPS functionality, you can use the 
// Command object to customize the messages that AMPS sends. This is useful for more advanced scenarios where you need 
// precise control over the command, or in cases where you need to use an earlier version of the client to communicate 
// with a more recent version of AMPS, or in cases where a named method is not available.
//
//     package main
//     
//     import (
//         "amps"
//         "fmt"
//     )
//     
//     
//     func main() {
//         client := amps.NewClient("my-application")
//     
//         err := client.Connect("tcp://localhost:9001/amps/json")
//         if err != nil { fmt.Println(err); return }
//     
//         err = client.Logon()
//         if err != nil { fmt.Println(err); return }
//     
//         var subscribeCommand = amps.NewCommand("subscribe").SetTopic("messages").SetFilter("/id > 20")
//     
//         _, err = client.ExecuteAsync(subscribeCommand, func(message *amps.Message) (msgErr error) {
//             // Print the message data
//             fmt.Println(string(message.Data()))
//             return
//         })
//     
//         if err != nil { fmt.Println("Subscribe Error:", err); return }
//     }
//
// This example provides the subscription to a 'messages' topic with a filter applied. 
//
// Everything You Need
//
// If you need more help getting started, open an issue in this repository.
package amps

import "fmt"


// AMPS Error constants
const (
	// AlreadyConnectedError - Connect() is called while there is already an active connection to AMPS.
	AlreadyConnectedError = iota

	// AuthenticationError - returned when the authentication was unsuccessful
	AuthenticationError

	// BadFilterError - returned when the AMPS server reports that a filter could not be successfully processed.
	BadFilterError

	// BadRegexTopicError - returned when the AMPS server reports that the regex provided as the topic for a 
	// subscription or query could not be processed. This most often indicates a syntax error in the regular expression.
	BadRegexTopicError

	// CommandError - returned when an error occurs in an AMPS command.
	CommandError

	// ConnectionError - returned when a general connection error occurred. 
	ConnectionError

	// ConnectionRefusedError - returned when the server is not available or refused to establish the connection.
	ConnectionRefusedError

	// DisconnectedError - returned when the client is not connected while attempting to perform an action 
	// that requires a working connection.
	DisconnectedError

	// ProtocolError - returned when a general AMPS protocol error occurred. 
	ProtocolError

	// InvalidTopicError - returned when the AMPS server reports that the command requested is not valid for the topic 
	// specified.
	InvalidTopicError

	// InvalidURIError - returned when the provided URI is not valid.
	InvalidURIError

	// nameInUseError ... 
	nameInUseError

	// NotEntitledError - returned when the provided username is not entitled to perform the action.
	NotEntitledError

	// RetryOperationError - returned when the attempt to retry to connect to AMPS using an Authenticator has failed.
	RetryOperationError

	// SubidInUseError - returned when the specified subscription ID is already in use for this client.
	SubidInUseError

	// SubscriptionAlreadyExistsError - returned when a client attempts to register a subscription that already exists.
	SubscriptionAlreadyExistsError

	// TimedOutError - returned when the time out occurs before getting the result of an action.
	TimedOutError

	// MessageHandlerError - returned when the message hander returned an error after processing the message.
	MessageHandlerError

	// UnknownError -  when AMPS reports an error of an unknown type, for example, when running an older version of 
	// the AMPS Go client against a more recent version of AMPS.
	UnknownError
)


func reasonToError(reason string) error {
	err := UnknownError

	switch reason {
	case "bad filter":
		err = BadFilterError
	case "invalid topic":
		err = InvalidTopicError
	case "not entitled":
		err = NotEntitledError
	case "auth failure":
		err = AuthenticationError
	case "bad regex":
		err = BadRegexTopicError
	}

	return NewError(err)
}


// NewError generates a new AMPS error, with optional description.
func NewError(errorCode int, message ...interface{}) error {
	var errorName string

	switch errorCode {
	case AlreadyConnectedError:
		errorName = "AlreadyConnectedError"
	case AuthenticationError:
		errorName = "AuthenticationError"
	case BadFilterError:
		errorName = "BadFilterError"
	case BadRegexTopicError:
		errorName = "BadRegexTopicError"
	case CommandError:
		errorName = "CommandError"
	case ConnectionError:
		errorName = "ConnectionError"
	case ConnectionRefusedError:
		errorName = "ConnectionRefusedError"
	case DisconnectedError:
		errorName = "DisconnectedError"
	case ProtocolError:
		errorName = "ProtocolError"
	case InvalidTopicError:
		errorName = "InvalidTopicError"
	case InvalidURIError:
		errorName = "InvalidURIError"
	case nameInUseError:
		errorName = "NameInUseError"
	case NotEntitledError:
		errorName = "NotEntitledError"
	case RetryOperationError:
		errorName = "RetryOperationError"
	case SubidInUseError:
		errorName = "SubidInUseError"
	case SubscriptionAlreadyExistsError:
		errorName = "SubscriptionAlreadyExistsError"
	case TimedOutError:
		errorName = "TimedOutError"
	case MessageHandlerError:
		errorName = "MessageHandlerError"
	default:
		errorName = "UnknownError"
	}

	if len(message) > 0 {
		return fmt.Errorf("%s: %s", errorName, message[0])
	}

	return fmt.Errorf("%s", errorName)
}
