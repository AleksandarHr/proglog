package main

import (
	"fmt"
	"log"

	"github.com/aleksandarhr/proglog/internal/server"
)

func main() {
	// simply create and start the server, passing the address to listen to
	port := ":8080"
	srv := server.NewHTTPServer(port)
	fmt.Println("Listening on port" + port)
	log.Fatal(srv.ListenAndServe())
}
