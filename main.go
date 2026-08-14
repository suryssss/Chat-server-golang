package main

// installing websockets library from github
// installing UUID library so we can assign each chat a unique id

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
)

// clientManager : keeps track of all the connected status of the users
// client : has a unique id and a socket connection and a message waiting to be send
// Message : contains two users as sender and receiver and also a message that is being send

type ClientManager struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

type Client struct {
	id     string
	socket *websocket.Conn
	send   chan []byte
}

type Message struct {
	Sender   string `json:"sender,omitempty"` //To increase the complexity using json instead of just storing in string
	Receiver string `json:"receiver,omitempty"`
	Content  string `json:"content,omitempty"`
}

var manager = ClientManager{
	broadcast:  make(chan []byte),
	register:   make(chan *Client),
	unregister: make(chan *Client),
	clients:    make(map[*Client]bool),
}

// the server gonna use three goroutines
// one for managing the clients
// one for reading websockets data
// one for writing websockets data

// read and write goroutines will get a new instance everytime they connect with a client

// a simple function to send messages

func (manager *ClientManager) start() {
	for {
		select {

		// everytime manager.register channel has data the client will be added to the map of avaliable clients by the client manager
		// and a message will be sent to the client saying that the client has been added

		case conn := <-manager.register:
			manager.clients[conn] = true
			jsonMessage, _ := json.Marshal(&Message{Content: "/A new socket has been added"})
			manager.send(jsonMessage, conn)

			// everytime manager.broadcast channel has data the client manager will send the data to all the clients
			// and if the client manager is not able to send the data to the client then the client will be removed from the map of avaliable clients

		case conn := <-manager.unregister:
			if _, ok := manager.clients[conn]; ok {
				delete(manager.clients, conn)
				jsonMessage, _ := json.Marshal(&Message{Content: "/A socket has been removed"})

				go func() {
					manager.broadcast <- jsonMessage
				}()
			}

			// every time manager.broadcast channel has data the client manager will send the data to all the clients
			// and if the client manager is not able to send the data to the client then the client will be removed from the map of avaliable clients

		case message := <-manager.broadcast:
			for conn := range manager.clients {
				select {
				case conn.send <- message:
				default:
					close(conn.send)
					delete(manager.clients, conn)
				}
			}
		}
	}
}

//to send messages to each of the client

func (manager *ClientManager) send(message []byte, ignore *Client) {
	for conn := range manager.clients {
		if conn != ignore {
			select {
			case conn.send <- message:
			default:
				close(conn.send)
				delete(manager.clients, conn)
			}
		}
	}
}

func (c *Client) read() {
	defer func() {
		manager.unregister <- c
		c.socket.Close()
	}()

	for {
		_, message, err := c.socket.ReadMessage()
		if err != nil {
			manager.unregister <- c
			c.socket.Close()
			break
		}
		jsonMessage, _ := json.Marshal(&Message{Sender: c.id, Content: string(message)})
		manager.broadcast <- jsonMessage
	}
}

func (c *Client) write() {
	defer func() {
		c.socket.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.socket.WriteMessage(websocket.CloseMessage, []byte(""))
				return
			}
			c.socket.WriteMessage(websocket.TextMessage, message)
		}
	}
}

func main() {
	fmt.Println("Starting the application")
	go manager.start()
	http.HandleFunc("/ws", wsPage)
	http.ListenAndServe(":12345", nil)
}

func wsPage(res http.ResponseWriter, req *http.Request) {
	conn, error := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(res, req, nil)
	if error != nil {
		http.NotFound(res, req)
		return
	}
	client := &Client{id: uuid.NewV4().String(), socket: conn, send: make(chan []byte)}

	manager.register <- client

	go client.read()
	go client.write()
}
