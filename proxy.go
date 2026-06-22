// 代理核心逻辑：
// 1. 解析客户端 Range 请求
// 2. 先下载首块，确认总大小并回写响应头
// 3. 按块并发拉取剩余数据
// 4. 按顺序写回客户端，尽量保持流式输出
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Player struct {
	client        *http.Client
	header        http.Header
	start         int64
	end           int64
	skip          int64
	thread        int
	chunkSize     int64
	url           string
	useHttpProxy  bool
}

// NewPlayer 根据上游请求头和代理参数创建一个下载器实例。
func NewPlayer(header http.Header, thread, chunkSizeKB int, url string, skip int64, useHttpProxy bool) *Player {
	client := buildHttpClient(useHttpProxy)

	h := http.Header{}
	for _, key := range []string{"User-Agent", "Cookie", "Referer"} {
		if v := header.Get(key); v != "" {
			h.Set(key, v)
		}
	}
	start, end := parseRange(header.Get("Range"))

	return &Player{
		client:       client,
		header:       h,
		start:        start,
		end:          end,
		skip:         skip,
		thread:       thread,
		chunkSize:    int64(chunkSizeKB) * 1024,
		url:          url,
		useHttpProxy: useHttpProxy,
	}
}

func buildHttpClient(useHttpProxy bool) *http.Client {
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       60 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableKeepAlives:     false,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	if useHttpProxy {
		transport.Proxy = http.ProxyFromEnvironment
	}
	return &http.Client{
		Timeout:   0,
		Transport: transport,
	}
}

// Play 执行一次完整的代理传输。
func (p *Player) Play(w http.ResponseWriter, ctx context.Context) error {
	s, e, err := p.downloadFirst(w, ctx)
	if err != nil {
		return err
	}
	fileSize := e + 1
	log.Printf("文件大小: %d MB, 线程: %d, 块大小: %d KB, skip: %d",
		fileSize/1024/1024, p.thread, p.chunkSize/1024, p.skip)

	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	results := make([][]byte, p.thread)
	var wg sync.WaitGroup

	for start := s; start < fileSize; start += int64(p.chunkSize) * int64(p.thread) {
		select {
		case <-ctx.Done():
			log.Printf("请求被取消")
			return ctx.Err()
		default:
		}

		activeThreads := 0
		chunkErrors := make([]error, p.thread)

		for i := 0; i < p.thread; i++ {
			chunkStart := start + int64(i)*p.chunkSize
			chunkEnd := chunkStart + p.chunkSize
			if chunkStart >= fileSize {
				break
			}
			if chunkEnd > fileSize {
				chunkEnd = fileSize
			}

			results[i] = nil
			chunkErrors[i] = nil
			activeThreads++
			wg.Add(1)

			go func(idx int, cs, ce int64) {
				defer wg.Done()

				downloadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()

				// 加 skip 偏移到请求位置，跳过伪装头部
				serverStart := cs + p.skip
				serverEnd := ce + p.skip

				var data []byte
				var err error
				for retry := 0; retry < 3; retry++ {
					data, _, _, err = p.downloadChunk(downloadCtx, serverStart, serverEnd, 3)
					if err == nil {
						break
					}
					log.Printf("块 %d-%d 第%d次重试", cs, ce-1, retry+1)
					time.Sleep(time.Second * time.Duration(retry+1))
				}

				if err != nil {
					log.Printf("⚠️ 块 %d-%d 下载彻底失败: %v", cs, ce-1, err)
					chunkErrors[idx] = fmt.Errorf("数据块 %d (%d-%d) 下载失败: %w", idx, cs, ce-1, err)
					return
				}
				results[idx] = data
			}(i, chunkStart, chunkEnd)
		}

		wg.Wait()

		for i := 0; i < activeThreads; i++ {
			if chunkErrors[i] != nil {
				log.Printf("❌ %v", chunkErrors[i])
				return chunkErrors[i]
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for i := 0; i < activeThreads; i++ {
			_, err = w.Write(results[i])
			if err != nil {
				log.Printf("写入失败: %v", err)
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	log.Printf("下载完成")
	return nil
}

// downloadFirst 下载首块数据，并根据源站返回的 Content-Range 确定完整文件大小。
func (p *Player) downloadFirst(w http.ResponseWriter, ctx context.Context) (int64, int64, error) {
	start, end := p.start, p.end
	if end <= 0 {
		end = 100
	} else {
		end += 1
	}
	end = start + min(end, p.chunkSize)

	// 应用 skip 偏移：向源站请求时跳过开头 N 字节
	skip := p.skip
	chunk, header, status, err := p.downloadChunk(ctx, start+skip, end+skip, 3)
	if err != nil {
		return 0, 0, err
	}

	matches := crRegex.FindStringSubmatch(header.Get("Content-Range"))
	if len(matches) != 4 {
		return 0, 0, errors.New("未获取到文件总大小")
	}
	totalLength, _ := strconv.ParseInt(matches[3], 10, 64)

	// 从总大小中减去 skip 部分，向客户端报告调整后的大小
	if skip > 0 && totalLength > skip {
		totalLength -= skip
	}

	if p.end <= 0 {
		end = totalLength - 1
	} else {
		end = p.end
	}

	h := w.Header()
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalLength))
	for k, v := range header {
		if k != "Content-Range" && k != "Content-Length" {
			h[k] = v
		}
	}
	w.WriteHeader(status)

	_, err = w.Write(chunk)
	if err != nil {
		return 0, 0, err
	}

	return start + int64(len(chunk)), end, nil
}

// downloadChunk 下载一个指定字节区间。
// 此处的 start/end 已经是应用了 skip 偏移后的值（即源站上的实际位置）。
func (p *Player) downloadChunk(ctx context.Context, start, end int64, maxRetries int) ([]byte, http.Header, int, error) {
	var lastErr error
	for retry := 0; retry < maxRetries; retry++ {
		req, err := http.NewRequestWithContext(ctx, "GET", p.url, nil)
		if err != nil {
			return nil, nil, -1, err
		}
		req.Header = p.header.Clone()
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			if retry < maxRetries-1 {
				select {
				case <-time.After(time.Duration(retry+1) * 500 * time.Millisecond):
					continue
				case <-ctx.Done():
					return nil, nil, -1, ctx.Err()
				}
			}
			continue
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == 206 || resp.StatusCode == 200 {
			return data, resp.Header, resp.StatusCode, nil
		}

		lastErr = fmt.Errorf("状态码: %d", resp.StatusCode)
	}

	return nil, nil, -1, fmt.Errorf("重试%d次失败: %v", maxRetries, lastErr)
}

// handleImage 代理图片下载，支持 useHttpProxy
func handleImage(w http.ResponseWriter, r *http.Request) {
	imageURL := r.URL.Query().Get("url")
	if imageURL == "" {
		http.Error(w, "url missing", http.StatusBadRequest)
		return
	}

	useHttpProxy := r.URL.Query().Get("useHttpProxy") == "1"
	client := buildHttpClient(useHttpProxy)

	req, err := http.NewRequestWithContext(r.Context(), "GET", imageURL, nil)
	if err != nil {
		http.Error(w, "request error", http.StatusBadRequest)
		return
	}
	if ua := r.Header.Get("User-Agent"); ua != "" {
		req.Header.Set("User-Agent", ua)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("图片代理失败: %v", err)
		http.Error(w, "proxy error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadGateway)
		return
	}

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(data)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

var crRegex = regexp.MustCompile(`bytes\s+(\d+)-(\d+)/(\d+)`)
var seRegex = regexp.MustCompile(`bytes=(\d+)-(\d*)`)

// parseRange 解析客户端传入的 Range 请求头。
func parseRange(rangeStr string) (int64, int64) {
	match := seRegex.FindStringSubmatch(rangeStr)
	if len(match) == 0 {
		return 0, -1
	}
	start, _ := strconv.ParseInt(match[1], 10, 64)
	end := int64(-1)
	if match[2] != "" {
		end, _ = strconv.ParseInt(match[2], 10, 64)
	}
	return start, end
}


