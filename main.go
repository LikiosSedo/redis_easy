package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"math/rand" // add this import
)

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
	ExpireAt time.Time 
}

// 判断当前条目是否已过期
func (e *Entry) isExpired() bool {
	if e.ExpireAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpireAt)
}

var cache sync.Map
var leaderboard sync.Map

func main() {
	// 根据命令行参数选择不同的运行模式
	if len(os.Args) > 1 {
		if os.Args[1] == "stress" {
			runAdvancedStressTest()
			return
		}
		if os.Args[1] == "leaderboard" {
			runLeaderboardTest()
			return
		}
	}

	// 启动 pprof 服务，方便性能分析（监听 :6060）
	go func() {
		log.Println("pprof server listening on :6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// 启动排行榜快照 HTTP 服务（监听 :8080）
	go func() {
		http.HandleFunc("/leaderboard", leaderboardSnapshotHandler)
		log.Println("Snapshot server listening on :8080")
		log.Fatal(http.ListenAndServe(":8080", nil))
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
		case "SREM":
			handleSRem(conn, request)
		case "HSET":
			handleHSet(conn, request)
		case "HGET":
			handleHGet(conn, request)
		case "HDEL":
			handleHDel(conn, request)
		case "LBADD":
			handleLBAdd(conn, request)
		case "LBTOP":
			handleLBTop(conn, request)
		case "LRANGE":
			handleLRange(conn, request)
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
	newElems := args[2:]
	list = append(newElems, list...)
	entry := &Entry{
		Type:     ListType,
		Value:    list,
		ExpireAt: time.Time{},
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
	popped := list[0]
	list = list[1:]
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
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(set)))
	for member := range set {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(member), member))
	}
	conn.Write([]byte(sb.String()))
}
// SREM 命令：从集合中删除一个或多个成员，返回删除的成员数量
func handleSRem(conn net.Conn, args []string) {
    if len(args) < 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'SREM' command\r\n"))
        return
    }
    key := args[1]
    val, ok := cache.Load(key)
    if !ok {
        // 键不存在，直接返回 0
        conn.Write([]byte(":0\r\n"))
        return
    }
    entry := val.(*Entry)
    if entry.isExpired() {
        cache.Delete(key)
        conn.Write([]byte(":0\r\n"))
        return
    }
    if entry.Type != SetType {
        conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
        return
    }
    set := entry.Value.(map[string]struct{})
    removed := 0
    // 遍历待删除的每个成员
    for _, member := range args[2:] {
        if _, exists := set[member]; exists {
            delete(set, member)
            removed++
        }
    }
    // 如果删除后集合为空，可以选择删除整个键
    if len(set) == 0 {
        cache.Delete(key)
    } else {
        // 更新存储中的集合
        entry.Value = set
        cache.Store(key, entry)
    }
    // 返回删除的成员数量
    conn.Write([]byte(fmt.Sprintf(":%d\r\n", removed)))
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
// HDEL 命令：删除哈希中一个或多个字段，返回成功删除的字段数
func handleHDel(conn net.Conn, args []string) {
    if len(args) < 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'HDEL' command\r\n"))
        return
    }
    key := args[1]
    val, ok := cache.Load(key)
    if !ok {
        // 如果 key 不存在，则删除字段数为 0
        conn.Write([]byte(":0\r\n"))
        return
    }
    entry := val.(*Entry)
    // 如果 key 已过期，则删除条目并返回 0
    if entry.isExpired() {
        cache.Delete(key)
        conn.Write([]byte(":0\r\n"))
        return
    }
    // 如果类型不是 HashType，则返回错误
    if entry.Type != HashType {
        conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
        return
    }
    hash := entry.Value.(map[string]string)

    // 统计成功删除的字段数
    deletedCount := 0
    for _, field := range args[2:] {
        if _, exists := hash[field]; exists {
            delete(hash, field)
            deletedCount++
        }
    }

    // 如果删完后 hash 为空，可选择删除整个 key
    if len(hash) == 0 {
        cache.Delete(key)
    } else {
        entry.Value = hash
        cache.Store(key, entry)
    }
    conn.Write([]byte(fmt.Sprintf(":%d\r\n", deletedCount)))
}

// LRANGE 命令：返回列表中从 start 到 stop 范围内的元素（stop 为闭区间）
func handleLRange(conn net.Conn, args []string) {
    if len(args) != 4 {
        conn.Write([]byte("-ERR wrong number of arguments for 'LRANGE' command\r\n"))
        return
    }
    key := args[1]
    startIdx, err1 := strconv.Atoi(args[2])
    stopIdx, err2 := strconv.Atoi(args[3])
    if err1 != nil || err2 != nil {
        conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
        return
    }
    // 获取列表数据
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
    if entry.Type != ListType {
        conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
        return
    }
    list := entry.Value.([]string)
    n := len(list)

    // 处理负索引：如果 start 或 stop 为负值，则从列表尾部计算偏移
    if startIdx < 0 {
        startIdx = n + startIdx
    }
    if stopIdx < 0 {
        stopIdx = n + stopIdx
    }
    // 修正起始和结束索引的边界
    if startIdx < 0 {
        startIdx = 0
    }
    if stopIdx < 0 {
        stopIdx = 0
    }
    if startIdx > n-1 {
        conn.Write([]byte("*0\r\n"))
        return
    }
    if stopIdx > n-1 {
        stopIdx = n - 1
    }
    if startIdx > stopIdx {
        conn.Write([]byte("*0\r\n"))
        return
    }
    sublist := list[startIdx : stopIdx+1]

    // 按 RESP 协议格式构造返回结果
    var sb strings.Builder
    sb.WriteString(fmt.Sprintf("*%d\r\n", len(sublist)))
    for _, item := range sublist {
        sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
    }
    conn.Write([]byte(sb.String()))
}


// LBADD 命令：更新或插入用户分数到排行榜
func handleLBAdd(conn net.Conn, args []string) {
    if len(args) != 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'LBADD' command\r\n"))
        return
    }
    user := args[1]
    score, err := strconv.Atoi(args[2])
    if err != nil {
        conn.Write([]byte("-ERR score must be an integer\r\n"))
        return
    }
    // 限制分数范围在 [0, 10000]
    if score > 10000 {
        score = 10000
    } else if score < 0 {
        score = 0
    }
    leaderboard.Store(user, score)
    conn.Write([]byte("+OK\r\n"))
}


// LBTOP 命令：返回排行榜前 N 名（返回 RESP 格式）
func handleLBTop(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'LBTOP' command\r\n"))
        return
    }
    topN, err := strconv.Atoi(args[1])
    if err != nil || topN <= 0 {
        conn.Write([]byte("-ERR N must be a positive integer\r\n"))
        return
    }
    var data []struct {
        User  string
        Score int
    }
    leaderboard.Range(func(key, value interface{}) bool {
        data = append(data, struct {
            User  string
            Score int
        }{key.(string), value.(int)})
        return true
    })
    // 按分数降序排序，如分数相同则按用户名升序
    sort.Slice(data, func(i, j int) bool {
        if data[i].Score == data[j].Score {
            return data[i].User < data[j].User
        }
        return data[i].Score > data[j].Score
    })
    if topN > len(data) {
        topN = len(data)
    }
    var sb strings.Builder
    sb.WriteString(fmt.Sprintf("*%d\r\n", topN*2))
    for i := 0; i < topN; i++ {
        user := data[i].User
        scoreStr := strconv.Itoa(data[i].Score)
        sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(user), user))
        sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(scoreStr), scoreStr))
    }
    conn.Write([]byte(sb.String()))
}


// HTTP handler: 实时生成排行榜快照页面，显示 Top20，并每 0.2s 自动刷新一次
func leaderboardSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	var data []struct {
		User  string
		Score int
	}
	leaderboard.Range(func(key, value interface{}) bool {
		data = append(data, struct {
			User  string
			Score int
		}{key.(string), value.(int)})
		return true
	})
	sort.Slice(data, func(i, j int) bool {
		return data[i].Score > data[j].Score
	})
	topN := 20
	if len(data) < topN {
		topN = len(data)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<html>
<head>
<meta http-equiv="refresh" content="0.2">
<title>Leaderboard Snapshot</title>
<style>
table { border-collapse: collapse; width: 50%%; }
th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
</style>
</head>
<body>
<h2>Leaderboard Snapshot (Top %d)</h2>
<table>
<tr><th>Rank</th><th>User</th><th>Score</th></tr>`, topN)
	for i := 0; i < topN; i++ {
		fmt.Fprintf(w, "<tr><td>%d</td><td>%s</td><td>%d</td></tr>", i+1, data[i].User, data[i].Score)
	}
	fmt.Fprint(w, `</table>
</body>
</html>`)
}

// runAdvancedStressTest 模拟缓存服务场景下的高并发读写：80% 请求热点数据、20% 请求随机数据
func runAdvancedStressTest() {
    // 调整并发连接数，减少对系统资源的瞬时冲击
    const clientCount = 1000
    const opsPerClient = 10000
    var wg sync.WaitGroup
    var totalOps int64   // 总操作数计数器
    var successOps int64 // 成功响应数计数器

    start := time.Now()

    for i := 0; i < clientCount; i++ {
        wg.Add(1)
        go func(clientID int) {
            defer wg.Done()

            // 初始建立连接，最多尝试 3 次
            const maxInitialRetries = 3
            var conn net.Conn
            var err error
            for r := 0; r < maxInitialRetries; r++ {
                conn, err = net.Dial("tcp", "127.0.0.1:6379")
                if err == nil {
                    break
                }
                log.Printf("Client %d: initial dial attempt %d error: %v\n", clientID, r+1, err)
                time.Sleep(50 * time.Millisecond)
            }
            if conn == nil {
                log.Printf("Client %d: failed to establish initial connection after %d attempts\n", clientID, maxInitialRetries)
                return
            }
            reader := bufio.NewReader(conn)

            for j := 0; j < opsPerClient; j++ {
                var key, cmd string
                if j%5 < 4 {
                    key = "hot_data"
                    if j%50 == 0 {
                        cmd = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key)
                    } else {
                        cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
                    }
                } else {
                    key = fmt.Sprintf("key_%d_%d", clientID, j)
                    if j%10 == 0 {
                        cmd = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$4\r\nval%d\r\n", len(key), key, j)
                    } else {
                        cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
                    }
                }

                const maxRetries = 3
                var opErr error
                var resp string

                // 每个操作最多尝试 maxRetries 次
                for attempt := 0; attempt < maxRetries; attempt++ {
                    // 如果连接为 nil，则尝试重新建立连接
                    if conn == nil {
                        conn, err = net.Dial("tcp", "127.0.0.1:6379")
                        if err != nil {
                            log.Printf("Client %d: re-dial error (attempt %d): %v\n", clientID, attempt+1, err)
                            time.Sleep(50 * time.Millisecond)
                            continue
                        }
                        reader = bufio.NewReader(conn)
                    }

                    // 发送命令
                    _, err = conn.Write([]byte(cmd))
                    if err != nil {
                        log.Printf("Client %d: write error (attempt %d): %v\n", clientID, attempt+1, err)
                        opErr = err
                        conn.Close()
                        conn = nil
                        time.Sleep(50 * time.Millisecond)
                        continue
                    }

                    // 记录本次操作
                    atomic.AddInt64(&totalOps, 1)
                    // 读取响应
                    resp, err = reader.ReadString('\n')
                    if err != nil {
                        log.Printf("Client %d: read error (attempt %d): %v\n", clientID, attempt+1, err)
                        opErr = err
                        conn.Close()
                        conn = nil
                        time.Sleep(50 * time.Millisecond)
                        continue
                    }
                    opErr = nil
                    break
                }
                if opErr == nil && len(resp) > 0 && resp[0] != '-' {
                    atomic.AddInt64(&successOps, 1)
                }
                // 中途暂停一下，模拟真实场景
                if j == opsPerClient/2 {
                    time.Sleep(100 * time.Millisecond)
                }
            }
            if conn != nil {
                conn.Close()
            }
        }(i)
    }
    wg.Wait()
    duration := time.Since(start)
    total := atomic.LoadInt64(&totalOps)
    success := atomic.LoadInt64(&successOps)
    successRatio := float64(success) / float64(total) * 100

    log.Printf("Advanced stress test completed: %d clients * %d ops in %v\n", clientCount, opsPerClient, duration)
    log.Printf("Total operations: %d, Successful responses: %d, Success ratio: %.2f%%\n", total, success, successRatio)
}


func runLeaderboardTest() {
	const clientCount = 100
	const opsPerClient = 10000
	var wg sync.WaitGroup

	start := time.Now()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
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
				player := fmt.Sprintf("player_%d", (clientID+j)%1000)
				score := rand.Intn(10001)
				cmd := fmt.Sprintf("*3\r\n$5\r\nLBADD\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n",
					len(player), player, len(strconv.Itoa(score)), score)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					log.Printf("Client %d: write LBADD error: %v\n", clientID, err)
					return
				}
				if _, err := reader.ReadString('\n'); err != nil {
					log.Printf("Client %d: read LBADD error: %v\n", clientID, err)
					return
				}
				if j%50 == 0 {
					topN := 5
					cmd = fmt.Sprintf("*2\r\n$5\r\nLBTOP\r\n$%d\r\n%d\r\n", len(strconv.Itoa(topN)), topN)
					if _, err := conn.Write([]byte(cmd)); err != nil {
						log.Printf("Client %d: write LBTOP error: %v\n", clientID, err)
						return
					}
					if _, err := reader.ReadString('\n'); err != nil {
						log.Printf("Client %d: read LBTOP error: %v\n", clientID, err)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	log.Printf("Leaderboard test completed: %d clients * %d ops in %v\n", clientCount, opsPerClient, duration)
}
