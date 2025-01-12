package main
import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)
var _ = net.Listen
var _ = os.Exit
func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	 handleConnection(conn)
}
func handleConnection(conn net.Conn) {
	defer conn.Close()
	for{
		// Read request
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Error reading request: ", err.Error())
			return
		}

		// Extract API version and correlation ID
		apiVersion := binary.BigEndian.Uint16(buff[6:8])
		correlationId := buff[8:12]
		fmt.Printf("Version requested %d\n", apiVersion)

		// First response part - header and error code
		length := []byte{0, 0, 0, 19}
		var versionError []byte
		switch apiVersion {
		case 0, 1, 2, 3, 4:
			versionError = []byte{0, 0}
		default:
			versionError = []byte{0, 35}
		}
		response := append(length, correlationId...)
		response = append(response, versionError...)
		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response: ", err.Error())
			return
		}

		// Second response part - API versions info
		apiVersionsResponse := []byte{
			2,     // Number of API keys + 1
			0, 18, // API key (18 for API_VERSIONS)
			0, 0, // Min version (0)
			0, 4, // Max version (4)
			0,          // TAG_BUFFER
			0, 0, 0, 0, // throttle_time_ms
			0, // TAG_BUFFER
		}

		_, err = conn.Write(apiVersionsResponse)
		if err != nil {
			fmt.Println("Error writing API versions response: ", err.Error())
			return
		}
	}
	
}