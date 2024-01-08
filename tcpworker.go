package tcpworker

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Worker represents a TCP worker
type Worker struct {
	Connection    net.Conn
	Name          string
	RequestChan  chan map[string]interface{}  // Channel for receiving requests
	closeChan     chan struct{}               // Channel for signaling closure
	ServerAddr    string
	ConnectionType string
	requestsMutex  sync.Mutex
}

const (
	TypeClient = "1"
	TypeWorker = "2"
)

// NewWorker creates a new TCP worker with a provided response channel
func NewWorker(name, serverAddr string, responseChan chan map[string]interface{}) (*Worker, error) {
	fmt.Println("TCP Worker V1.1.1")
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to server: %s", err)
	}

	// Send the worker name to the server
	workerNameMessage := map[string]interface{}{"ServiceName": name, "Type": TypeWorker}
	initialMessage, _ := json.Marshal(workerNameMessage)
	_, err =conn.Write(initialMessage)
	if err != nil {
		return nil, fmt.Errorf("error connecting: %s", err)
	}

	// Initialize channels and maps
	closeChan := make(chan struct{})
	// Create the worker instance
	c := &Worker{
		Connection:    conn,
		Name:          name,
		RequestChan:  responseChan,
		closeChan:     closeChan,
		ServerAddr:    serverAddr,
		ConnectionType: TypeWorker,
		requestsMutex: sync.Mutex{},
	}

	// Start a goroutine to listen for responses from the server
	if err := c.handleRequests(); err != nil {
		return nil, fmt.Errorf("error starting response handling: %s", err)
	}

	return c, nil
}

// Reconnect attempts to reconnect the worker to the server
func (c *Worker) reconnect() error {
	conn, err := net.Dial("tcp", c.ServerAddr)
	if err != nil {
		return fmt.Errorf("error reconnecting to server: %s", err)
	}

	// Send the worker name to the server
	workerNameMessage := map[string]interface{}{"ServiceName": c.Name, "Type": c.ConnectionType}
	initialMessage, _ := json.Marshal(workerNameMessage)
	_,err = conn.Write(initialMessage)
	if err != nil {
		return fmt.Errorf("error connecting: %s", err)
	}

	// Update the connection in the Worker struct
	c.Connection = conn

	return nil
}

// SendMessage sends a JSON message to the server
func (c *Worker) SendResponse(response map[string]interface{}) error {
	//fmt.Printf("Starting send of:%v\n",response)
	// Encode the message as JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %s", err)
	}

	//fmt.Println("Write to socket")
	_, err =c.Connection.Write(jsonResponse)
	if err != nil {
		return fmt.Errorf("error sending: %s", err)
	}

	return nil
}

// Function to handle responses from the server
func (c *Worker) handleRequests() error {
	connectionEstablished := true

	go func() {
		for {
			if connectionEstablished {
				decoder := json.NewDecoder(c.Connection)
				//fmt.Println("Received message on TCP/IP")
				var requestMessage map[string]interface{}
				//fmt.Println("Before decoder")
				if err := decoder.Decode(&requestMessage); err != nil {
					fmt.Printf("TCP/IP Decoder error:%s\n",err.Error())
					c.Connection.Close()
					connectionEstablished = false
				} else {
					c.RequestChan <- requestMessage
					//fmt.Println("Send response on general response channel")
				}
			} else {
				// Reconnect here using the receiver 'c'
				err := c.reconnect()
				if err != nil {
					fmt.Printf("Error reconnecting to server: %s\n", err)
					fmt.Println("Start sleeping")
					time.Sleep(10 * time.Second)
					fmt.Println("Wake up!")
				} else {
					connectionEstablished = true
					fmt.Println("Reconnected")
				}
			}
		}	
	}()

	return nil
}


