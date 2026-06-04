package main

type Join_sign struct {
	Kind      string `json:"kind"`
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
}

type Message struct {
	ClientID  string `json:"client_id"`
	Text      string `json:"text"`
	TimeStamp string `json:"time_stamp"`
}

type History struct {
	Messages []Message
}
