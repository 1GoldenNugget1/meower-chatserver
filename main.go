package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// Client represents a connected user.
type Client struct {
	conn net.Conn
	ch   chan string
	name string
}

type listRequest struct {
	client *Client
}

// Server manages all active clients and message routing.
type Server struct {
	clients   map[*Client]bool
	join      chan *Client
	leave     chan *Client
	broadcast chan string
	listReq   chan listRequest
}

func newServer() *Server {
	return &Server{
		clients:   make(map[*Client]bool),
		join:      make(chan *Client),
		leave:     make(chan *Client),
		broadcast: make(chan string),
		listReq:   make(chan listRequest),
	}
}

// Run listens on channels to safely manage state and fan-out messages.
func (s *Server) Run() {
	for {
		select {
		case client := <-s.join:
			s.clients[client] = true
			msg := fmt.Sprintf("--> %s joined the chat\n", client.name)
			s.announce(msg)

		case client := <-s.leave:
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.ch)
				msg := fmt.Sprintf("<-- %s left the chat\n", client.name)
				s.announce(msg)
			}

		case msg := <-s.broadcast:
			s.announce(msg)

		case req := <-s.listReq:
			var names []string
			for c := range s.clients {
				names = append(names, c.name)
			}
			msg := fmt.Sprintf("Connected users (%d): %s\n", len(names), strings.Join(names, ", "))
			select {
			case req.client.ch <- msg:
			default:
			}
		}
	}
}

// announce sends a message to all active client channels.
func (s *Server) announce(msg string) {
	for client := range s.clients {
		select {
		case client.ch <- msg:
		default:
			// If a client buffer is full, disconnect to avoid blocking the server loop
			client.conn.Close()
		}
	}
}

// handleConnection manages an individual client lifecycle.
func (s *Server) handleConnection(conn net.Conn) {
	// Prompt the client for a display name
	conn.Write([]byte("Enter your name: "))
	reader := bufio.NewReader(conn)
	nameInput, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}
	name := strings.TrimSpace(nameInput)
	if name == "" {
		name = conn.RemoteAddr().String()
	}

	client := &Client{
		conn: conn,
		ch:   make(chan string, 10),
		name: name,
	}

	// Register client with the server
	s.join <- client

	defer func() {
		// Deregister client upon exit
		s.leave <- client
		conn.Close()
	}()

	// Writer goroutine: sends messages from client.ch to the TCP connection
	go func() {
		for msg := range client.ch {
			_, err := conn.Write([]byte(msg))
			if err != nil {
				break
			}
		}
	}()

	// Reader loop: reads incoming lines from the TCP connection
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break // Client disconnected or error occurred
		}

		cleanLine := strings.TrimSpace(line)
		if cleanLine == "" {
			continue
		}

		// Command handling
		if strings.HasPrefix(cleanLine, "/") {
			parts := strings.SplitN(cleanLine, " ", 2)
			cmd := parts[0]

			switch cmd {
			case "/test":
				s.broadcast <- "Test command received.\n"
				return
			case "/help":
				client.ch <- "Available commands:\n" +
					"  /nick [new_name] - change your display name\n" +
					"  /list            - list all connected users\n" +
					"  /exit            - disconnect from the chat\n" +
					"  /help            - show this help message\n" +
					"  /test            - test command (does nothing)\n"

			case "/exit":
				client.ch <- "Goodbye!\n"
				return
			case "/list":
				s.listReq <- listRequest{client: client}

			case "/nick":
				if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
					client.ch <- "Usage: /nick [new_name]\n"
				} else {
					newName := strings.TrimSpace(parts[1])
					oldName := client.name
					client.name = newName
					s.broadcast <- fmt.Sprintf("--> %s is now known as %s\n", oldName, newName)
				}

			default:
				client.ch <- fmt.Sprintf("Unknown command: %s\n", cmd)
			}
			continue
		}

		msg := fmt.Sprintf("[%s]: %s\n", client.name, cleanLine)
		s.broadcast <- msg
	}
}

func main() {
	server := newServer()
	go server.Run()

	listener, err := net.Listen("tcp", ":2190")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	log.Println("TCP Chat Server running on port 2190...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go server.handleConnection(conn)
	}
}
