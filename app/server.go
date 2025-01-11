package main
import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)
func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		defer conn.Close()
		var correlationID int32 = 7
		response := make([]byte, 8) // Allocate 8 bytes for message_size and correlation_id
		binary.BigEndian.PutUint32(response[0:4], 4) // message_size is 4 bytes
		binary.BigEndian.PutUint32(response[4:8], uint32(correlationID)) // correlation_id
		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
			continue
		}
	}
}