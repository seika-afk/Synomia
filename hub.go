package main

import (
	"sync"
)

type Session struct {
	ID       string
	Messages []Message
	Clients  map[*Client]bool

	Broadcast chan []byte

	Register   chan *Client
	Unregister chan *Client
	Mu         sync.Mutex
}

type SessionManager struct {
	Sessions map[string]*Session
	Mu       sync.Mutex
}

func newSession(id string) *Session {
	return &Session{
		ID:       id,
		Messages: make([]Message, 0),
		Clients:  make(map[*Client]bool),

		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
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
	m.Sessions[id] = session
	go session.run()
	return session
}

func (s *Session) run() {
	for {
		select {
		case client := <-s.Register:
			s.Mu.Lock()
			s.Clients[client] = true
			s.Mu.Unlock()
		case client := <-s.Unregister:
			s.Mu.Lock()
			if _, ok := s.Clients[client]; ok {
				delete(s.Clients, client)
				close(client.send)
				s.Mu.Unlock()
			}
		case message := <-s.Broadcast:
			s.Mu.Lock()
			for client := range s.Clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(s.Clients, client)
				}
			}
			s.Mu.Unlock()

		}
	}
}
