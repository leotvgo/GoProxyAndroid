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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

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

		skip := int64(0)
		if skipStr := params.Get("skip"); skipStr != "" {
			skip, err = strconv.ParseInt(skipStr, 10, 64)
			if err != nil {
				http.Error(w, "skip必须为整数", http.StatusBadRequest)
				return
			}
		}

		useHttpProxy := params.Get("useHttpProxy") == "1"

		player := NewPlayer(r.Header, t, c, url, skip, useHttpProxy)
		if err := player.Play(w, r.Context()); err != nil {
			log.Printf("播放错误: %v", err)
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "healthy", "type": "go", "port": %d, "timestamp": "%s"}`, *port, time.Now().Format(time.RFC3339))
	})

	log.SetOutput(os.Stdout)
	log.Printf("服务器启动在 " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
