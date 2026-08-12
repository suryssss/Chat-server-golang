package main

// installing websockets library from github
// installing UUID library so we can assign each chat a unique id

import (
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
	Send   chan []byte
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

func (manager *ClientManager) start() {

}
