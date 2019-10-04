package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
	"kafka-exporter/overseer"
	"kafka-exporter/rotatefilehook"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

type Configuration struct {
	Brokers []string `json:"brokers"`
}

const (
	appVer = "v1.4 29082019"
)

var (
	addr            string
	endpoint        *url.URL
	port            int
	zkServers       string
	memlimit        uint64
	isdebug         bool
	dummy           bool
	topicsFilter    string
	cluster         string
	consumerMetrics bool
)

func prog(state overseer.State) {
	flag.IntVar(&port, "port", 9098, "The port to serve the endpoint from.")
	flag.StringVar(&zkServers, "zk.servers", "", "Comma separated list of zkServers in the format host:port")
	flag.StringVar(&topicsFilter, "topics.filter", "", "Regex expression to export only topics that match expression")
	flag.StringVar(&cluster, "cluster.name", "", "Kafka cluster name")
	flag.Uint64Var(&memlimit, "mem.limit", 419430400, "Memory limit in bytes, default: 400MB") //default 400MB
	flag.BoolVar(&isdebug, "debug", false, "Enable/disable debug mode")
	flag.BoolVar(&dummy, "kafka-exporter-slave", true, "Just a dummy flag")

	flag.BoolVar(&consumerMetrics, "enable.consumer.metrics", false, "Turn on consumer metrics")

	required := []string{"zk.servers", "cluster.name"}
	flag.Parse()

	seen := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range required {
		if !seen[req] {
			// or possibly use `log.Fatalf` instead of:
			fmt.Fprintf(os.Stderr, "Missing required -%s argument/flag\n", req)
			os.Exit(2) // the same exit code flag.Parse uses
		}
	}

	rotateFileHook, err := rotatefilehook.NewRotateFileHook(rotatefilehook.RotateFileConfig{
		Filename:   cluster + ".log",
		MaxSize:    30, // megabytes
		MaxBackups: 10,
		MaxAge:     28, //days
		Level:      logrus.InfoLevel,
		Formatter: &logrus.JSONFormatter{
			TimestampFormat: time.RFC822,
		},
	})
	if err != nil {
		log.Errorf("RotateFileHook init error.")
	}
	log.AddHook(rotateFileHook)

	log.Infoln("kafka-exporter customized by chinhnc " + appVer)

	appConf := Configuration{}
	err = gonfig.GetConf("conf/"+cluster+".conf", &appConf)
	if err != nil {
		log.Errorln("Failed to read config file.", err)
		panic(err)
	}

	log.Infoln("Broker List:" + strings.Join(appConf.Brokers, ","))

	c := KafkaCollector(&zkServers, &topicsFilter, &cluster, appConf.Brokers)
	prometheus.MustRegister(c)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err = w.Write([]byte(`<html>
			<head><title>Kafka Exporter</title></head>
			<body>
			<h1>Kafka Exporter</h1>
			<p><a href=/metrics>Metrics</a></p>
			</body>
			</html>`))
		if err != nil {
			log.Errorln("Failed handling writer", err)
		}
	})

	if isdebug == true {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%v", port),
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		_ = <-sigs
		log.Infoln("main: received SIGINT or SIGTERM, shutting down")
		context, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(context); err != nil {
			log.Infoln("main: failed to shutdown endpoint with err=%#v\n", err)
		}
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Infoln("main: failure while serving endpoint, err=%#v\n", err)
	}

}

func main() {
	overseer.Run(overseer.Config{
		Program:   prog,
		Debug:     true,
		NoRestart: false,
	})
}
