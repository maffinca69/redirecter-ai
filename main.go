package main

import (
	"bytes"
	"context"
	"expvar"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/http2"
)

// Пул HTTP клиентов
var clientPool = sync.Pool{
	New: func() interface{} {
		return &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
					DualStack: true,
				}).DialContext,
			},
			Timeout: 30 * time.Second,
		}
	},
}

// Пул буферов
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 32*1024)) // 32KB начальный размер
	},
}

// Кастомная ошибка HTTP
type httpError struct {
	message string
	code    int
}

func (e *httpError) Error() string {
	return e.message
}

// Метрики
var (
	requestsTotal = expvar.NewInt("proxy_requests_total")
	requestErrors = expvar.NewMap("proxy_request_errors")
)

// Буферизированный writer ответов
type bufferedResponseWriter struct {
	http.ResponseWriter
	buf    *bytes.Buffer
	status int
}

func (w *bufferedResponseWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *bufferedResponseWriter) WriteHeader(status int) {
	w.status = status
}

// Проверка валидности URL
func isValidURL(rawURL string) bool {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return false
	}
	return (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

// Копирование заголовков
func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// Обработчик прокси-запросов
func proxyHandler(w http.ResponseWriter, r *http.Request) {
	requestsTotal.Add(1)

	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()

	// Получаем клиента и буфер из пулов
	client := clientPool.Get().(*http.Client)
	defer clientPool.Put(client)

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	bw := &bufferedResponseWriter{
		ResponseWriter: w,
		buf:            buf,
		status:         http.StatusOK,
	}

	// Обработка в горутине
	done := make(chan struct{})
	go func() {
		defer close(done)

		rawURL := r.URL.Query().Get("url")
		if rawURL == "" {
			requestErrors.Add("missing_url", 1)
			http.Error(bw, "Missing 'url' parameter", http.StatusBadRequest)
			return
		}

		if !isValidURL(rawURL) {
			requestErrors.Add("invalid_url", 1)
			http.Error(bw, "Invalid URL", http.StatusBadRequest)
			return
		}

		proxyReq, err := http.NewRequestWithContext(ctx, r.Method, rawURL, r.Body)
		if err != nil {
			requestErrors.Add("create_request", 1)
			http.Error(bw, err.Error(), http.StatusInternalServerError)
			return
		}

		copyHeaders(proxyReq.Header, r.Header)

		resp, err := client.Do(proxyReq)
		if err != nil {
			requestErrors.Add("upstream_error", 1)
			http.Error(bw, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		copyHeaders(bw.Header(), resp.Header)
		bw.WriteHeader(resp.StatusCode)
		_, err = io.Copy(bw, resp.Body)
		if err != nil {
			log.Printf("Error copying response: %v", err)
		}
	}()

	select {
	case <-done:
		if bw.status != 0 {
			w.WriteHeader(bw.status)
		}
		_, err := io.Copy(w, bw.buf)
		if err != nil {
			log.Printf("Error writing response: %v", err)
		}
	case <-ctx.Done():
		requestErrors.Add("timeout", 1)
		http.Error(w, "Request timeout", http.StatusGatewayTimeout)
	}
}

func init() {
	// Инициализация метрик памяти с уникальным именем
	expvar.Publish("proxy_memstats", expvar.Func(func() interface{} {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m
	}))
}

func main() {
	// Инициализация HTTP/2
	http.DefaultTransport.(*http.Transport).DialContext = (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext

	if err := http2.ConfigureTransport(http.DefaultTransport.(*http.Transport)); err != nil {
		log.Fatalf("Failed to configure HTTP/2: %v", err)
	}

	// Настройка сервера
	server := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Лимит одновременных запросов
			sem := make(chan struct{}, 100)
			sem <- struct{}{}
			defer func() { <-sem }()

			proxyHandler(w, r)
		}),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  90 * time.Second,
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Fatalf("Server shutdown error: %v", err)
		}
	}()

	log.Println("Starting server on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
