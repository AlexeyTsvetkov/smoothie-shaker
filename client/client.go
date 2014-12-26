package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"io"
)

const maxClients int = 100

type Client struct {
	id     int
	reader *bufio.Reader
	writer *bufio.Writer
}

func (client *Client) Run() {
	msg := fmt.Sprintf("Hi! My name is %d\n", client.id)
	_, err := client.writer.WriteString(msg)
	client.writer.Flush()

	if err != nil {
		log.Printf("Client %d write error: %s", client.id, err)
		return
	}

	response := ""

	for {
		line, err := client.reader.ReadString('\n')

		if err == nil {
			response += line
		} else if err == io.EOF {
			break
		} else {
			log.Printf("Client %d got error in response: %s", client.id, err)
			return
		}
	}

	log.Printf("Client %d got: %s", client.id, response)
}

func RunClient(id int, out chan<- int) {
	conn, err := net.Dial("tcp", "localhost:5432")

	if err != nil {
		log.Printf("Could not create client %d: %s", id, err)
		return
	}

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	client := &Client{
		id:     id,
		reader: reader,
		writer: writer,
	}

	client.Run()
	conn.Close()
	out <- id
}

func main() {
	id := 0
	clients := 0
	out := make(chan int, maxClients)
	
	for {
		for clients < maxClients {
			id += 1
			clients += 1
			go RunClient(id, out)
		}

		select {
		case <-out:
			clients -= 1
		}
	}
}
