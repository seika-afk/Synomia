package main

type Join_sign struct {
	Kind      string `json:"kind"`
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
}

type Message struct {
	ClientID string `json:"client_id"`
	Content  string `json:"content`
}
