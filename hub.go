package main

import (
	"encoding/json"
	"log"
	"sync"
)

type Session struct {
	manager  *SessionManager
	ID       string
	Messages []Message
	Clients  map[*Client]bool

	Broadcast       chan broadcastMessage
	TypingIndicator chan Typing_indicator

	Register   chan *Client
	Unregister chan *Client
	done       chan struct{}
	Mu         sync.Mutex
}

type SessionManager struct {
	Sessions map[string]*Session
	Mu       sync.Mutex
}

type broadcastMessage struct {
	sender  *Client
	payload []byte
}

func newSession(id string) *Session {
	return &Session{
		done:     make(chan struct{}),
		ID:       id,
		Messages: make([]Message, 0),
		Clients:  make(map[*Client]bool),

		Broadcast:       make(chan broadcastMessage, 256),
		TypingIndicator: make(chan Typing_indicator, 256),
		Register:        make(chan *Client, 256),
		Unregister:      make(chan *Client, 256),
	}
}

func (m *SessionManager) getSession(id string) *Session {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	session, exists := m.Sessions[id]
	if exists {
		return session
	}
	session = newSession(id)
	session.manager = m
	m.Sessions[id] = session
	go session.run()
	return session
}

func (m *SessionManager) removeSession(id string) {
	m.Mu.Lock()
	delete(m.Sessions, id)
	m.Mu.Unlock()
}

func (s *Session) run() {
	for {
		select {
		case client := <-s.Register:
			s.Mu.Lock()
			s.Clients[client] = true
			close(client.registered)
			s.Mu.Unlock()
		case client := <-s.Unregister:
			s.Mu.Lock()
			if _, ok := s.Clients[client]; ok {
				delete(s.Clients, client)
				close(client.send)
			}
			empty := len(s.Clients) == 0
			recipients := make([]*Client, 0, len(s.Clients))
			for c := range s.Clients {
				if c == client {
					continue
				}
				recipients = append(recipients, c)
			}

			if empty {
				s.manager.removeSession(s.ID)
				close(s.done)
				s.Mu.Unlock()
				return
			}
			dis_msg := Join_sign{Kind: "Disconnect", ClientID: client.id, SessionID: client.session.ID}
			payload, err := json.Marshal(dis_msg)
			if err != nil {
				log.Printf("ignoring malformed message ")

				s.Mu.Unlock()
				continue
			}
			s.Mu.Unlock()
			for _, c := range recipients {
				select {
				case c.send <- payload:
				default:
					close(c.send)
					s.Mu.Lock()
					delete(s.Clients, c)
					s.Mu.Unlock()
				}
			}
		case broadcast := <-s.Broadcast:
			var msg Message
			err := json.Unmarshal(broadcast.payload, &msg)
			if err != nil {
				log.Printf("ignoring malformed message ")
				continue
			}

			s.Mu.Lock()
			s.Messages = append(s.Messages, msg)
			for client := range s.Clients {
				if client == broadcast.sender {
					continue
				}
				select {
				case client.send <- broadcast.payload:
				default:
					close(client.send)
					delete(s.Clients, client)
				}
			}
			empty := len(s.Clients) == 0
			s.Mu.Unlock()

			if empty {
				s.manager.removeSession(s.ID)
				close(s.done)
				return
			}

		case typ := <-s.TypingIndicator:
			payload, _ := json.Marshal(typ)
			s.Mu.Lock()
			for client := range s.Clients {
				if client.username == typ.Username {
					continue
				}
				select {
				case client.send <- payload:
				default:
					close(client.send)
					delete(s.Clients, client)
				}
			}
			empty := len(s.Clients) == 0
			s.Mu.Unlock()
			if empty {
				s.manager.removeSession(s.ID)
				close(s.done)
				return
			}
		}

	}
}
