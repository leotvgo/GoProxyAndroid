// 独立运行模式下的入口。
// 这个文件用于直接启动一个本地 HTTP 代理服务，便于单独调试 Go 代理逻辑。
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := flag.Int("port", 5576, "listen port")
	flag.Parse()
	addr := fmt.Sprintf(":%d", *port)

	// 根路径用于最简单的存活探测。
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	// /proxy 是核心代理入口，必须携带线程数、分块大小和目标地址。
	http.HandleFunc("/proxy", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		thread, chunkSize, url := params.Get("thread"), params.Get("chunkSize"), params.Get("url")

		if thread == "" || chunkSize == "" || url == "" {
			http.Error(w, "参数不完整", http.StatusBadRequest)
			return
		}

		t, err := strconv.Atoi(thread)
		if err != nil {
			http.Error(w, "thread必须为整数", http.StatusBadRequest)
			return
		}
		c, err := strconv.Atoi(chunkSize)
		if err != nil {
			http.Error(w, "chunkSize必须为整数", http.StatusBadRequest)
			return
		}

		player := NewPlayer(r.Header, t, c, url)

		if err := player.Play(w, r.Context()); err != nil {
			log.Printf("播放错误: %v", err)
		}
	})

	// 健康检查接口。
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "healthy", "type": "go", "port": %d, "timestamp": "%s"}`, *port, time.Now().Format(time.RFC3339))
	})

	log.SetOutput(os.Stdout)
	log.Printf("服务器启动在 " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
