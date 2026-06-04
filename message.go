package main

type Join_sign struct {
	Kind      string `json:"kind"`
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	UserName  string `json:"user_name"`
}

type Message struct {
	UserName  string `json:"user_name"`
	Text      string `json:"text"`
	TimeStamp string `json:"time_stamp"`
}

type History struct {
	Messages []Message
}

type Typing_indicator struct {
	Username string `json:"user_name"`
	Typing   bool   `json:"typing"`
}
