package main

// installing websockets library from github
// installing UUID library so we can assign each chat a unique id

import (
	"encoding/json"

	"github.com/gorilla/websocket"
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

func (manager *ClientManager) send(message []byte, client *Client) {
	client.send <- message
}

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

				manager.broadcast <- jsonMessage
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
