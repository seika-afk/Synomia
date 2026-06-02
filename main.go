package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

var addr = flag.String("addr", ":4000", "Http service address")

func main() {
	flag.Parse()
	fmt.Println("ChatServer Started at : ws//localhost:", *addr, "/chat")
	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		serveWs(w, r)
	})
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal(err)
	}
}
