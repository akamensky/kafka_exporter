package main

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/klauspost/compress/gzip"
	"golang.org/x/exp/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	cfg, err := parseConfig()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	slog.Info("Starting kafka_exporter")

	k, err := newKafka(cfg.kafka)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	go func() {
		for true {
			k.collect()
			time.Sleep(cfg.kafka.metadataRefreshInterval)
		}
	}()

	go cacheResponse(15 * time.Second)

	http.HandleFunc(cfg.http.context, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "gzip")
		if len(metricsData) > 0 {
			gw := gzip.NewWriter(w)
			defer gw.Close()
			_, err := gw.Write(metricsData)
			if err != nil {
				slog.Error("Error while handling request", slog.String("error", err.Error()))
			}
		}
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + cfg.http.context + `'>Metrics</a></p>
	        </body>
	        </html>`))
		if err != nil {
			slog.Error("Error while handling request", slog.String("error", err.Error()))
		}
	})

	if cfg.http.tlsEnable {
		_, err := canReadCertAndKey(cfg.http.tlsCertFile, cfg.http.tlsKeyFile)
		if err != nil {
			slog.Error("error reading server cert and key", slog.String("error", err.Error()))
			os.Exit(1)
		}

		clientAuthType := tls.NoClientCert
		if cfg.http.tlsUseMutualAuth {
			clientAuthType = tls.RequireAndVerifyClientCert
		}

		certPool := x509.NewCertPool()
		if cfg.http.tlsCaFile != "" {
			if caCert, err := os.ReadFile(cfg.http.tlsCaFile); err == nil {
				certPool.AppendCertsFromPEM(caCert)
			} else {
				slog.Error("error reading server ca", slog.String("error", err.Error()))
				os.Exit(1)
			}
		}

		server := &http.Server{
			Addr: cfg.http.addr,
			TLSConfig: &tls.Config{
				ClientCAs:        certPool,
				ClientAuth:       clientAuthType,
				MinVersion:       tls.VersionTLS12,
				CurvePreferences: []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
				CipherSuites: []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
					tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
					tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_RSA_WITH_AES_256_CBC_SHA,
					tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
				},
			},
		}

		slog.Info("Listening on HTTPS", slog.String("addr", cfg.http.addr))
		if err := server.ListenAndServeTLS(cfg.http.tlsCertFile, cfg.http.tlsKeyFile); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	} else {
		slog.Info("Listening on HTTP", slog.String("addr", cfg.http.addr))
		if err := http.ListenAndServe(cfg.http.addr, nil); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	}
}

func cacheResponse(interval time.Duration) {
	for true {
		metricsData = []byte(exporter.Render())

		time.Sleep(interval)
	}
}
