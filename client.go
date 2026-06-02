package main

import "github.com/gorilla/websocket"

type Client struct {
	session *Session
	id      string
	conn    *websocket.Conn
	send    chan []byte
}
