package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	session    *Session
	id         string
	conn       *websocket.Conn
	send       chan []byte
	registered chan struct{}
}

const (
	writeWait      = 10 * time.Second
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "http://localhost:3000" || origin == ""
	},
}

func (c *Client) readPump() {
	defer func() {
		select {
		case c.session.Unregister <- c:
		case <-c.session.done:
		}
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		var message Message

		err = json.Unmarshal(msg, &message)
		if err == nil {
			select {
			case c.session.Broadcast <- broadcastMessage{sender: c, payload: msg}:
			case <-c.session.done:
				return
			}
		}
	}

}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:

			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		}
	}

}

func serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	var join Join_sign
	err = conn.ReadJSON(&join)
	if err != nil || join.Kind != "join" || join.SessionID == "" || join.ClientID == "" {
		conn.Close()
		if err != nil {
			log.Printf("invalid join payload: %v", err)
		}
		return
	}
	log.Printf("client joined session=%s client_id=%s ", join.SessionID, join.ClientID)

	session := sm.getSession(join.SessionID)

	client := &Client{session: session, id: join.ClientID, conn: conn, send: make(chan []byte, 256), registered: make(chan struct{})}
	client.session.Register <- client
	<-client.registered

	go client.writePump()
	go client.readPump()

	joinBytes, err := json.Marshal(join)
	if err != nil {
		return
	}
	session.Mu.Lock()
	historyMessages := append([]Message(nil), session.Messages...)
	session.Mu.Unlock()

	message_hist_json, err := json.Marshal(History{Messages: historyMessages})
	if err != nil {
		return
	}

	select {
	case client.send <- message_hist_json:
	case <-session.done:
		return
	}

	session.Mu.Lock()
	for otherClient := range session.Clients {
		if otherClient == client {
			continue
		}
		select {
		case otherClient.send <- joinBytes:
		default:
			close(otherClient.send)
			delete(session.Clients, otherClient)
		}
	}
	session.Mu.Unlock()
}
