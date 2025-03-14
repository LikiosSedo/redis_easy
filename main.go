package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 定义数据类型
type DataType int

const (
	StringType DataType = iota
	ListType
	SetType
	HashType
)

// Entry 表示存储在缓存中的一个条目，包含数据类型、实际值以及过期时间（ExpireAt 为零值表示不过期）
type Entry struct {
	Type     DataType
	Value    interface{}
	ExpireAt time.Time // 过期时间，零值表示不过期
}

// 判断当前条目是否已过期
func (e *Entry) isExpired() bool {
	if e.ExpireAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpireAt)
}

// 全局缓存，key 为 string，对应的值为 *Entry
var cache sync.Map

func main() {
	// 如果传入 "stress" 参数则启动压力测试
	if len(os.Args) > 1 && os.Args[1] == "stress" {
		runStressTest()
		return
	}

	// 启动 pprof 服务，方便使用 Go 内置工具进行性能分析
	go func() {
		log.Println("pprof server listening on :6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// 启动 TCP 服务监听 6379 端口
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		log.Println("Closing connection:", conn.RemoteAddr())
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	for {
		request, err := readCommand(reader)
		if err != nil {
			if err == net.ErrClosed || err.Error() == "EOF" {
				log.Println("Client disconnected:", conn.RemoteAddr())
			} else {
				log.Println("Error reading command:", err)
			}
			return
		}
		if request == nil || len(request) == 0 {
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
		case "TTL":
			handleTTL(conn, request)
		case "LPUSH":
			handleLPush(conn, request)
		case "LPOP":
			handleLPop(conn, request)
		case "SADD":
			handleSAdd(conn, request)
		case "SMEMBERS":
			handleSMembers(conn, request)
		case "HSET":
			handleHSet(conn, request)
		case "HGET":
			handleHGet(conn, request)
		case "QUIT":
			conn.Write([]byte("+OK\r\n"))
			return
		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", request[0])))
		}
	}
}

// readCommand 解析客户端发送的命令，支持 RESP 和 inline 格式
func readCommand(reader *bufio.Reader) ([]string, error) {
	prefix, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}

	if prefix[0] == '*' {
		// RESP 数组格式
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSuffix(line, "\r\n")
		count, convErr := strconv.Atoi(line[1:])
		if convErr != nil {
			return nil, fmt.Errorf("protocol error: invalid bulk count")
		}
		args := make([]string, 0, count)
		for i := 0; i < count; i++ {
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
			data := make([]byte, bulkLen)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				return nil, err
			}
			// 丢弃后面的 CRLF
			if _, err := reader.Discard(2); err != nil {
				return nil, err
			}
			args = append(args, string(data))
		}
		return args, nil
	} else {
		// inline 格式
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSuffix(line, "\r\n")
		if line == "" {
			return nil, nil
		}
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

// ======================
// 命令实现部分
// ======================

// GET 命令：返回指定键对应的字符串值
func handleGet(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
		return
	}
	key := args[1]
	val, ok := cache.Load(key)
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	entry := val.(*Entry)
	// 检查是否过期
	if entry.isExpired() {
		cache.Delete(key)
		conn.Write([]byte("$-1\r\n"))
		return
	}
	if entry.Type != StringType {
		conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
		return
	}
	strVal := fmt.Sprintf("%v", entry.Value)
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(strVal), strVal)))
}

// SET 命令：设置字符串键值，并支持 EX/PX 选项设置过期时间
// 用法示例：SET key value [EX seconds | PX milliseconds]
func handleSet(conn net.Conn, args []string) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
		return
	}
	key := args[1]
	value := args[2]
	var expireDuration time.Duration = 0

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
			ms, err := strconv.Atoi(args[4])
			if err != nil {
				conn.Write([]byte("-ERR invalid PX expiration value\r\n"))
				return
			}
			expireDuration = time.Duration(ms) * time.Millisecond
		}
	}
	var expireAt time.Time
	if expireDuration > 0 {
		expireAt = time.Now().Add(expireDuration)
	}
	entry := &Entry{
		Type:     StringType,
		Value:    value,
		ExpireAt: expireAt,
	}
	cache.Store(key, entry)
	conn.Write([]byte("+OK\r\n"))
}

// DEL 命令：删除一个或多个键
func handleDel(conn net.Conn, args []string) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
		return
	}
	count := 0
	for _, key := range args[1:] {
		// 若键存在且未过期，则删除之
		val, ok := cache.Load(key)
		if ok {
			entry := val.(*Entry)
			if entry.isExpired() {
				cache.Delete(key)
			} else {
				cache.Delete(key)
				count++
			}
		}
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
}

// TTL 命令：返回指定键剩余的生存时间（单位秒）
// 若键不存在返回 -2，若键存在但没有设置过期返回 -1
func handleTTL(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'TTL' command\r\n"))
		return
	}
	key := args[1]
	val, ok := cache.Load(key)
	if !ok {
		conn.Write([]byte(":-2\r\n"))
		return
	}
	entry := val.(*Entry)
	if entry.isExpired() {
		cache.Delete(key)
		conn.Write([]byte(":-2\r\n"))
		return
	}
	if entry.ExpireAt.IsZero() {
		conn.Write([]byte(":-1\r\n"))
		return
	}
	ttl := int(entry.ExpireAt.Sub(time.Now()).Seconds())
	if ttl < 0 {
		ttl = 0
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", ttl)))
}

// LPUSH 命令：向列表左侧插入一个或多个元素，并返回列表的新长度
func handleLPush(conn net.Conn, args []string) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPUSH' command\r\n"))
		return
	}
	key := args[1]
	var list []string
	val, ok := cache.Load(key)
	if ok {
		entry := val.(*Entry)
		if entry.isExpired() {
			cache.Delete(key)
		} else if entry.Type != ListType {
			conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			return
		} else {
			list = entry.Value.([]string)
		}
	}
	// 将新元素插入列表头部
	newElems := args[2:]
	list = append(newElems, list...)
	entry := &Entry{
		Type:     ListType,
		Value:    list,
		ExpireAt: time.Time{}, // 继承原有过期逻辑可在此扩展
	}
	cache.Store(key, entry)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(list))))
}

// LPOP 命令：弹出列表左侧的一个元素
func handleLPop(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPOP' command\r\n"))
		return
	}
	key := args[1]
	val, ok := cache.Load(key)
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	entry := val.(*Entry)
	if entry.isExpired() {
		cache.Delete(key)
		conn.Write([]byte("$-1\r\n"))
		return
	}
	if entry.Type != ListType {
		conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
		return
	}
	list := entry.Value.([]string)
	if len(list) == 0 {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	// 弹出第一个元素
	popped := list[0]
	list = list[1:]
	// 更新存储
	if len(list) == 0 {
		cache.Delete(key)
	} else {
		entry.Value = list
		cache.Store(key, entry)
	}
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(popped), popped)))
}

// SADD 命令：向集合中添加一个或多个成员，返回新增的成员数
func handleSAdd(conn net.Conn, args []string) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'SADD' command\r\n"))
		return
	}
	key := args[1]
	var set map[string]struct{}
	val, ok := cache.Load(key)
	if ok {
		entry := val.(*Entry)
		if entry.isExpired() {
			cache.Delete(key)
		} else if entry.Type != SetType {
			conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			return
		} else {
			set = entry.Value.(map[string]struct{})
		}
	}
	if set == nil {
		set = make(map[string]struct{})
	}
	added := 0
	for _, member := range args[2:] {
		if _, exists := set[member]; !exists {
			set[member] = struct{}{}
			added++
		}
	}
	entry := &Entry{
		Type:  SetType,
		Value: set,
	}
	cache.Store(key, entry)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", added)))
}

// SMEMBERS 命令：返回集合中的所有成员
func handleSMembers(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'SMEMBERS' command\r\n"))
		return
	}
	key := args[1]
	val, ok := cache.Load(key)
	if !ok {
		conn.Write([]byte("*0\r\n"))
		return
	}
	entry := val.(*Entry)
	if entry.isExpired() {
		cache.Delete(key)
		conn.Write([]byte("*0\r\n"))
		return
	}
	if entry.Type != SetType {
		conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
		return
	}
	set := entry.Value.(map[string]struct{})
	// 构造 RESP 数组格式返回
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(set)))
	for member := range set {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(member), member))
	}
	conn.Write([]byte(sb.String()))
}

// HSET 命令：设置哈希中指定字段的值，返回新增字段数（更新时返回 0）
func handleHSet(conn net.Conn, args []string) {
	if len(args) != 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'HSET' command\r\n"))
		return
	}
	key := args[1]
	field := args[2]
	value := args[3]
	var hash map[string]string
	val, ok := cache.Load(key)
	if ok {
		entry := val.(*Entry)
		if entry.isExpired() {
			cache.Delete(key)
		} else if entry.Type != HashType {
			conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			return
		} else {
			hash = entry.Value.(map[string]string)
		}
	}
	if hash == nil {
		hash = make(map[string]string)
	}
	_, exists := hash[field]
	hash[field] = value
	entry := &Entry{
		Type:  HashType,
		Value: hash,
	}
	cache.Store(key, entry)
	if exists {
		conn.Write([]byte(":0\r\n"))
	} else {
		conn.Write([]byte(":1\r\n"))
	}
}

// HGET 命令：获取哈希中指定字段的值
func handleHGet(conn net.Conn, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'HGET' command\r\n"))
		return
	}
	key := args[1]
	field := args[2]
	val, ok := cache.Load(key)
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	entry := val.(*Entry)
	if entry.isExpired() {
		cache.Delete(key)
		conn.Write([]byte("$-1\r\n"))
		return
	}
	if entry.Type != HashType {
		conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
		return
	}
	hash := entry.Value.(map[string]string)
	value, exists := hash[field]
	if !exists {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
}

// ======================
// 简单的压力测试代码
// ======================

// runStressTest 模拟多个客户端并发发送 SET/GET 命令
func runStressTest() {
	const clientCount = 100
	const opsPerClient = 1000
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				log.Printf("Client %d: connection error: %v\n", clientID, err)
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			for j := 0; j < opsPerClient; j++ {
				// 构造一个简单的 SET 命令：SET key_<clientID> value_<j>
				key := fmt.Sprintf("key_%d", clientID)
				value := fmt.Sprintf("value_%d", j)
				cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
				conn.Write([]byte(cmd))
				// 读取响应
				_, err := reader.ReadString('\n')
				if err != nil {
					log.Printf("Client %d: error reading SET response: %v\n", clientID, err)
					return
				}
				// 然后执行 GET 命令
				cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
				conn.Write([]byte(cmd))
				_, err = reader.ReadString('\n')
				if err != nil {
					log.Printf("Client %d: error reading GET response: %v\n", clientID, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	log.Printf("Stress test completed: %d clients * %d ops in %v\n", clientCount, opsPerClient*2, duration)
}
