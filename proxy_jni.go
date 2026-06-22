package main

/*
#include <stdlib.h>
#include <jni.h>

static jstring NewJString(JNIEnv* env, const char* s) {
    if (s == NULL) s = "";
    return (*env)->NewStringUTF(env, s);
}
*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

var (
	server        *http.Server
	serverMu      sync.Mutex
	serverRunning bool
	serverPort    int
	lastError     string
	routesOnce    sync.Once
)

func setLastError(msg string) {
	serverMu.Lock()
	lastError = msg
	serverMu.Unlock()
}

func clearLastError() {
	setLastError("")
}

//export Java_com_github_catvod_spider_GoProxyLibrary_startProxy
func Java_com_github_catvod_spider_GoProxyLibrary_startProxy(env *C.JNIEnv, clazz C.jclass, cPort C.jint) C.jint {
	port := int(cPort)
	if port <= 0 {
		port = 5576
	}
	clearLastError()

	serverMu.Lock()
	if serverRunning {
		serverMu.Unlock()
		setLastError("proxy already running")
		return C.jint(1)
	}
	serverMu.Unlock()

	routesOnce.Do(setupRoutes)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 300 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		log.Printf("Go代理端口绑定失败: %v", err)
		setLastError(err.Error())
		return C.jint(2)
	}

	serverMu.Lock()
	server = srv
	serverRunning = true
	serverPort = port
	serverMu.Unlock()

	go func(localServer *http.Server, listener net.Listener, localPort int) {
		log.SetOutput(os.Stdout)
		log.Printf("Go代理服务启动在 :%d", localPort)
		if err := localServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("Go代理服务错误: %v", err)
			setLastError(err.Error())
		}
		serverMu.Lock()
		if server == localServer {
			serverRunning = false
			server = nil
			serverPort = 0
		}
		serverMu.Unlock()
	}(srv, ln, port)

	return C.jint(0)
}

//export Java_com_github_catvod_spider_GoProxyLibrary_stopProxy
func Java_com_github_catvod_spider_GoProxyLibrary_stopProxy(env *C.JNIEnv, clazz C.jclass) C.jint {
	serverMu.Lock()
	localServer := server
	running := serverRunning
	serverMu.Unlock()

	if !running || localServer == nil {
		clearLastError()
		return C.jint(0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := localServer.Shutdown(ctx); err != nil {
		log.Printf("Go代理停止错误: %v", err)
		setLastError(err.Error())
		return C.jint(1)
	}

	serverMu.Lock()
	if server == localServer {
		serverRunning = false
		server = nil
		serverPort = 0
	}
	serverMu.Unlock()
	log.Printf("Go代理服务已停止")
	clearLastError()
	return C.jint(0)
}

//export Java_com_github_catvod_spider_GoProxyLibrary_isProxyRunning
func Java_com_github_catvod_spider_GoProxyLibrary_isProxyRunning(env *C.JNIEnv, clazz C.jclass) C.jint {
	serverMu.Lock()
	defer serverMu.Unlock()
	if serverRunning {
		return C.jint(1)
	}
	return C.jint(0)
}

//export Java_com_github_catvod_spider_GoProxyLibrary_getLastError
func Java_com_github_catvod_spider_GoProxyLibrary_getLastError(env *C.JNIEnv, clazz C.jclass) C.jstring {
	serverMu.Lock()
	msg := lastError
	serverMu.Unlock()
	cmsg := C.CString(msg)
	defer C.free(unsafe.Pointer(cmsg))
	return C.NewJString(env, cmsg)
}

func setupRoutes() {
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
		serverMu.Lock()
		listenPort := serverPort
		serverMu.Unlock()
		if listenPort <= 0 {
			listenPort = 5575
		}
		fmt.Fprintf(w, `{"status": "healthy", "type": "go", "port": %d, "timestamp": "%s"}`, listenPort, time.Now().Format(time.RFC3339))
	})
}

func init() {}
