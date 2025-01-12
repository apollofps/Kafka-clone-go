package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Listen on port 9092
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	defer listener.Close()

	// Continuously accept new connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		// Handle each connection concurrently
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read request into a buffer
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Error reading request:", err)
			return
		}

		// If the read is empty, client likely closed the connection
		if n == 0 {
			fmt.Println("Client closed connection")
			return
		}

		// Extract API version (2 bytes) and Correlation ID (4 bytes)
		apiVersion := binary.BigEndian.Uint16(buff[6:8])
		correlationId := buff[8:12]
		fmt.Printf("Version requested %d\n", apiVersion)

		// Construct response: length (4 bytes), correlation ID (4 bytes), error code (2 bytes)
		// length in this example is just set to 19 (fake total message length) for demonstration
		length := []byte{0, 0, 0, 19}

		var versionError []byte
		switch apiVersion {
		case 0, 1, 2, 3, 4:
			versionError = []byte{0, 0}  // no error
		default:
			versionError = []byte{0, 35} // unknown server error code
		}

		// First part of response: <message length><correlationId><errorCode>
		response := append(length, correlationId...)
		response = append(response, versionError...)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
			return
		}

		// Second response part: API versions info
		// For APIVersions (key=18), we need to ensure max version >= 4
		apiVersionsResponse := []byte{
			2,           // Number of API keys + 1
			0, 18,       // API key (18 for API_VERSIONS)
			0, 0,        // Min version
			0, 4,        // Max version
			0,           // TAG_BUFFER
			0, 0, 0, 0,  // throttle_time_ms
			0,           // TAG_BUFFER
		}

		_, err = conn.Write(apiVersionsResponse)
		if err != nil {
			fmt.Println("Error writing API versions response:", err)
			return
		}
	}
}
