package main

import (
    "bufio"
    "fmt"
    "net"
    "log"
    "strconv"
    "strings"
    "sync"
    "time"
    "io"
)

// Global in-memory store
var cache sync.Map

func main() {
    // Start a TCP server on port 6379 (localhost)
    listener, err := net.Listen("tcp", ":6379")
    if err != nil {
        log.Fatal("Error starting TCP server:", err)
    }
    log.Println("Server is listening on 0.0.0.0:6379")
    
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Println("Failed to accept connection:", err)
            continue
        }
        log.Println("New client connected:", conn.RemoteAddr())
        // Handle the connection in a new goroutine
        go handleConnection(conn)
    }
}

func handleConnection(conn net.Conn) {
    defer func() {
        // Ensure the connection is closed when this function ends
        log.Println("Closing connection:", conn.RemoteAddr())
        conn.Close()
    }()

    reader := bufio.NewReader(conn)
    for {
        // Read the next command from the client
        request, err := readCommand(reader)
        if err != nil {
            if err == net.ErrClosed || err.Error() == "EOF" {
                // Client closed the connection
                log.Println("Client disconnected:", conn.RemoteAddr())
            } else {
                log.Println("Error reading command:", err)
            }
            return // exit the loop and end the goroutine
        }
        if request == nil {
            // No command was parsed (e.g., empty line), continue to next read
            continue
        }

        // request is a slice of strings: [command, arg1, arg2, ...]
        if len(request) == 0 {
            continue
        }
        cmd := strings.ToUpper(request[0])
        switch cmd {
        case "GET":
            handleGet(conn, request)
        case "SET":
            handleSet(conn, request)
        case "DEL":
            handleDel(conn, request)
        case "QUIT":
            // Respond and then exit the handler
            conn.Write([]byte("+OK\r\n"))
            return  // break out of the loop to close connection
        default:
            // Unknown command
            conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", request[0])))
        }
    }
}

// readCommand reads the next Redis command from the client and returns it as a slice of strings.
func readCommand(reader *bufio.Reader) ([]string, error) {
    // Peek at the first byte to decide format without consuming it
    prefix, err := reader.Peek(1)
    if err != nil {
        return nil, err // e.g., EOF
    }

    if prefix[0] == '*' {
        // RESP Array format
        // Read the whole line after '*' (e.g., "*3\r\n")
        line, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        // line now contains "*<N>\r\n"
        line = strings.TrimSuffix(line, "\r\n")
        count, convErr := strconv.Atoi(line[1:])  // parse number after '*'
        if convErr != nil {
            return nil, fmt.Errorf("protocol error: invalid bulk count")
        }
        // Read each bulk string
        args := make([]string, 0, count)
        for i := 0; i < count; i++ {
            // Read the length line (e.g., "$3\r\n")
            lengthLine, err := reader.ReadString('\n')
            if err != nil {
                return nil, err
            }
            lengthLine = strings.TrimSuffix(lengthLine, "\r\n")
            if len(lengthLine) == 0 || lengthLine[0] != '$' {
                return nil, fmt.Errorf("protocol error: expected bulk string")
            }
            bulkLen, err := strconv.Atoi(lengthLine[1:])
            if err != nil {
                return nil, fmt.Errorf("protocol error: invalid bulk length")
            }
            // Read the actual bulk data of length bulkLen, plus the trailing CRLF
            data := make([]byte, bulkLen)
            _, err = io.ReadFull(reader, data)
            if err != nil {
                return nil, err
            }
            // Discard the CRLF after the bulk data
            if _, err := reader.Discard(2); err != nil {
                return nil, err
            }
            args = append(args, string(data))
        }
        return args, nil
    } else {
        // Inline command format (not starting with '*')
        line, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        // Remove CRLF and split by whitespace
        line = strings.TrimSuffix(line, "\r\n")
        if line == "" {
            return nil, nil // empty line (keep-alive ping or just CRLF)
        }
        // Handle quoted strings in inline commands:
        // We will do a simple parse: if a token starts with " and ends with ", treat it as one argument with spaces inside.
        var parts []string
        inQuote := false
        current := ""
        for _, r := range line {
            if r == ' ' && !inQuote {
                if current != "" {
                    parts = append(parts, current)
                    current = ""
                }
            } else if r == '"' {
                // toggle quoted section
                inQuote = !inQuote
            } else {
                current += string(r)
            }
        }
        if current != "" {
            parts = append(parts, current)
        }
        return parts, nil
    }
}

func handleGet(conn net.Conn, args []string) {
    if len(args) != 2 {
        // Wrong number of arguments for GET
        conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
        return
    }
    key := args[1]
    value, ok := cache.Load(key)
    if !ok {
        // Key not found -> return nil bulk string
        conn.Write([]byte("$-1\r\n"))
    } else {
        // Key found -> return the value as bulk string
        strVal := fmt.Sprintf("%v", value)  // ensure it's a string
        conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(strVal), strVal)))
    }
}

func handleSet(conn net.Conn, args []string) {
    if len(args) < 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
        return
    }
    key := args[1]
    value := args[2]
    // By default, no expiration
    var expireDuration time.Duration = 0

    // Check for expiration options
    if len(args) >= 5 {
        opt := strings.ToUpper(args[3])
        if opt == "EX" {
            seconds, err := strconv.Atoi(args[4])
            if err != nil {
                conn.Write([]byte("-ERR invalid EX expiration value\r\n"))
                return
            }
            expireDuration = time.Duration(seconds) * time.Second
        } else if opt == "PX" {
            milliseconds, err := strconv.Atoi(args[4])
            if err != nil {
                conn.Write([]byte("-ERR invalid PX expiration value\r\n"))
                return
            }
            expireDuration = time.Duration(milliseconds) * time.Millisecond
        }
        // (If an unknown option or missing value is provided, you could return an error. 
        // For simplicity, we ignore other options like NX/XX in this basic implementation.)
    }

    // Store the key-value in the map
    cache.Store(key, value)
    // If an expiration is set, launch a goroutine to delete the key after the duration
    if expireDuration > 0 {
        go func(k string, d time.Duration) {
            time.Sleep(d)
            cache.Delete(k)
        }(key, expireDuration)
    }
    // Respond with OK
    conn.Write([]byte("+OK\r\n"))
}

func handleDel(conn net.Conn, args []string) {
    if len(args) < 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
        return
    }
    count := 0
    for _, key := range args[1:] {
        _, existed := cache.LoadAndDelete(key)
        if existed {
            count++
        }
    }
    // Respond with an integer reply of how many keys were deleted
    conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
}
