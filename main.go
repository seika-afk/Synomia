package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

var addr = flag.String("addr", ":4000", "Http service address")
var sm = SessionManager{
	Sessions: make(map[string]*Session),
}

func main() {
	flag.Parse()
	fmt.Println("ChatServer Started at : ws//localhost", *addr, "/chat")
	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		serveWs(w, r)
	})
	err := http.ListenAndServe("0.0.0.0:4000", nil)
	if err != nil {
		log.Fatal(err)
	}
}
