package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/shirou/gopsutil/process"
	"kafka-exporter/Shopify/sarama"
	"kafka-exporter/kazoo-go"
	"kafka-exporter/overseer"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

type PartitionStats struct {
	Topic            *kazoo.Topic
	Partition        *kazoo.Partition
	Offset           int64
	OldOffset        int64
	ISR              []int32
	Replicas         []int32
	Leader           int32
	Underrepplicated bool
	Unavailable      bool
	ResponseTime     time.Duration
	ZkResponseTime   time.Duration
}

type collector struct {
	sync.RWMutex
	zkServers         string
	cluster           string
	topicsFilter      string
	brokerList        []string
	deadPartitionList [] string
	deadBrokers       []string
	panicTest         map[string]*prometheus.GaugeVec

	up             *prometheus.Desc
	scrapeFailures *prometheus.Desc
	failureCount   int
	kafkaMetrics   map[string]*prometheus.GaugeVec
	client         *http.Client
	zkClient       *kazoo.Kazoo
	brokerClient   sarama.Client
}

const (
	kafkaNamespace = "kafka"
)

var (
	is_restarting bool
	is_running    bool
)

func bool2float(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func SetRestartState(state bool) {
	is_restarting = state
}

func newFuncMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(kafkaNamespace, "", metricName), docString, labels, nil)
}

func KafkaCollector(zkservers *string, topicsFilter *string, cluster *string, brokerList []string) *collector {
	metrics := map[string]*prometheus.GaugeVec{}

	return &collector{
		zkServers:      *zkservers,
		cluster:        *cluster,
		topicsFilter:   *topicsFilter,
		brokerList:     brokerList,
		up:             newFuncMetric("up", "Able to contact zk", []string{}),
		scrapeFailures: newFuncMetric("scrape_failures_total", "Number of errors while scraping kafka metrics", []string{}),
		kafkaMetrics:   metrics,
	}
}

func (c *collector) initClients() {

	log.Infoln("Init zookeeper client with connection string: ", c.zkServers)
	var err error

	if c.zkClient != nil {
		err = c.zkClient.Close()
		if (err != nil) {
			log.Errorln(err)
		}
	}
	if c.brokerClient != nil {
		if !c.brokerClient.Closed() {
			err = c.brokerClient.Close()
			if (err != nil) {
				log.Errorln(err)
			}
		}
	}

	c.zkClient, err = kazoo.NewKazooFromConnectionString(c.zkServers, nil)
	if err != nil {
		log.Errorln("Error Init zookeeper client with connection string:", c.zkServers, err)
		return
	}

	brokers, err := c.zkClient.BrokerList()
	if err != nil {
		log.Errorln("Error reading brokers from zk", err)
		return
	}

	log.Infoln("Init Kafka Client with Brokers:", brokers)
	config := sarama.NewConfig()
	config.ClientID = "kafka-exporter"
	config.Net.DialTimeout = 500 * time.Millisecond
	config.Net.ReadTimeout = 500 * time.Millisecond
	config.Net.WriteTimeout = 3 * time.Second
	c.brokerClient, err = sarama.NewClient(brokers, config)

	if err != nil {
		log.Errorln("Error Init Kafka Client", err)
		return
	}
	log.Infoln("Done Init Clients")
}

func initMetrics(metrics map[string]*prometheus.GaugeVec) map[string]*prometheus.GaugeVec {

	metrics["topic_current_offset"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_current_offset",
			Help:      "Current offset of kafka topic",
		},

		[]string{"cluster", "topic",
		},
	)

	metrics["topic_oldest_offset"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_oldest_offset",
			Help:      "Current offset of kafka topic",
		},

		[]string{"cluster", "topic",
		},
	)

	metrics["cluster_nodes"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "cluster_total_nodes",
			Help:      "Total number of nodes",
		},

		[]string{"cluster",
		},
	)
	metrics["cluster_current_nodes"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "cluster_current_nodes",
			Help:      "Current number of nodes",
		},

		[]string{"cluster",
		},
	)

	metrics["topic_partition_current_isr"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_current_isr",
			Help:      "Current isr of a topic partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["topic_partition_current_offset"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_current_offset",
			Help:      "Current offset of a topic partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["topic_partition_oldest_offset"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_oldest_offset",
			Help:      "Oldest offset of a topic partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["topic_partition_current_replicas"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_current_replicas",
			Help:      "Current replicas of a topic partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["topic_partition_current_leader"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_current_leader",
			Help:      "Current leader of a topic partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["topic_partition_meta_read_time"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_partition_meta_read_time",
			Help:      "Time to read metadata from broker",
		},

		[]string{"cluster", "topic", "partition", "broker",
		},
	)

	metrics["topic_zk_read_time"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_zk_read_time",
			Help:      "Time to read metadata from zookeeper",
		},

		[]string{"cluster", "topic", "partition", "broker",
		},
	)

	metrics["cluster_current_controller"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "cluster_current_controller",
			Help:      "Current broker being controller of kafka cluster",
		},

		[]string{"cluster", "broker",
		},
	)

	metrics["under_replicated_partition"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "under_replicated_partition",
			Help:      "Under replicated partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["consumer_group_partition_offset"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "consumer_group_partition_offset",
			Help:      "Consumer group offset of a partition",
		},

		[]string{"cluster", "consumergroup", "topic", "partition",
		},
	)

	metrics["consumer_group_partition_lag"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "consumer_group_partition_lag",
			Help:      "Consumer group lag of a partition",
		},

		[]string{"cluster", "consumergroup", "topic", "partition",
		},
	)

	metrics["topic_unavailable_partition"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "topic_unavailable_partition",
			Help:      "Unavailable partition",
		},

		[]string{"cluster", "topic", "partition",
		},
	)

	metrics["cluster_dead_broker"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "dead_broker",
			Help:      "Current dead broker",
		},

		[]string{"cluster", "broker",
		},
	)

	metrics["brokers_monitor_fails"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "brokers_monitor_fails",
			Help:      "Failed to get brokerlist from zookeeper",
		},

		[]string{"cluster",
		},
	)

	metrics["broker_not_response"] = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: kafkaNamespace,
			Name:      "broker_response_error",
			Help:      "Failed to get offset from broker",
		},

		[]string{"cluster", "broker",
		},
	)

	return metrics
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.scrapeFailures
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorln("=====Recover in collector:%s\n", err)
			log.Errorln("Stack trace:")
			log.Errorln(string(debug.Stack()))
		}
	}()

	if !is_running {
		c.scrape()
	}

	c.collectMetrics(ch)
	return
}

func (c *collector) scrape() {
	log.Infoln(fmt.Sprintf("Initializing scrape thread."))
	is_running = true
	for {
		c.resetMetrics()
		log.Infoln(fmt.Sprintf("Collecting metrics"))
		pid := os.Getpid()
		//fmt.Println(pid)
		proc, err := process.NewProcess(int32(pid))
		if err == nil {
			meminfo, _ := proc.MemoryInfoEx()
			//auto restart
			if meminfo != nil {
				if (meminfo.RSS > memlimit) {
					if !is_restarting {
						log.Infoln(fmt.Sprintf("kafka exporter is restarting due to mem limit reached, current value = (  %.6f MB)", meminfo.RSS/1024/1024))
						overseer.Restart()
						SetRestartState(true)
					}
				}
			}
		}
		
		c.initClients()

		c.kafkaMetrics = initMetrics(c.kafkaMetrics)
		c.updateMetrics()
		time.Sleep(30 * time.Second)
	}
}

func (c *collector) getPartitionStats(topic *kazoo.Topic, partition *kazoo.Partition, ch chan<- *PartitionStats, wg *sync.WaitGroup, metrics map[string]*prometheus.GaugeVec, oldConsumerGroups kazoo.ConsumergroupList) {
	defer wg.Done()
	//log.Warnln("Topic: " + topic.Name)
	if (topic != nil && partition != nil) {
		stats, err := c.getPartitionInfo(topic, partition)

		if err != nil {
			log.Errorln("Failed to fetch partition stats for topic " + topic.Name)
			stats = nil
		}

		topicLabel := map[string]string{"cluster": c.cluster, "topic": stats.Topic.Name, "partition": strconv.Itoa(int(stats.Partition.ID))}

		c.Lock()
		metrics["topic_partition_current_isr"].With(topicLabel).Set(float64(len(stats.ISR)))
		metrics["topic_partition_current_replicas"].With(topicLabel).Set(float64(len(stats.Replicas)))
		metrics["topic_partition_current_leader"].With(topicLabel).Set(float64(stats.Leader))
		metrics["under_replicated_partition"].With(topicLabel).Set(bool2float(stats.Underrepplicated))
		if stats.Unavailable {
			metrics["topic_unavailable_partition"].With(topicLabel).Set(1)
		}
		metrics["topic_partition_current_offset"].With(topicLabel).Set(float64(stats.Offset))
		metrics["topic_partition_oldest_offset"].With(topicLabel).Set(float64(stats.OldOffset))
		metrics["topic_partition_meta_read_time"].With(prometheus.Labels{"cluster": c.cluster, "topic": stats.Topic.Name, "partition": strconv.Itoa(int(stats.Partition.ID)), "broker": strconv.Itoa(int(stats.Leader))}).Set(float64(stats.ResponseTime))
		metrics["topic_zk_read_time"].With(prometheus.Labels{"cluster": c.cluster, "topic": stats.Topic.Name, "partition": strconv.Itoa(int(stats.Partition.ID)), "broker": strconv.Itoa(int(stats.Leader))}).Set(float64(stats.ZkResponseTime))
		c.Unlock()
		//topicOffset += stat.Offset
		//topicOldestOffset += stat.OldOffset

		if (consumerMetrics == true) {
			//
			for _, group := range oldConsumerGroups {
				offset, _ := group.FetchOffset(stats.Topic.Name, stats.Partition.ID)
				if offset > 0 {
					consumerGroupLabels := map[string]string{"cluster": c.cluster, "consumergroup": group.Name, "topic": stats.Topic.Name, "partition": strconv.Itoa(int(stats.Partition.ID))}
					consumerGroupLag := stats.Offset - offset
					c.Lock()
					c.kafkaMetrics["consumer_group_partition_offset"].With(consumerGroupLabels).Set(float64(offset))
					c.kafkaMetrics["consumer_group_partition_lag"].With(consumerGroupLabels).Set(float64(consumerGroupLag))
					c.Unlock()
				}

			}
		}

		ch <- stats
	} else {
		log.Warnln("Topic or partition is nil")
	}
}

func (c *collector) getPartitionInfo(topic *kazoo.Topic, partition *kazoo.Partition) (*PartitionStats, error) {
	var stats PartitionStats
	var brokerIds []int32
	zkStartTime := time.Now()
	brokerIds, err := c.zkClient.BrokerIDList()

	stats.Partition = partition
	stats.Topic = topic

	leader, err := partition.Leader()

	if err != nil {
		log.Errorln("Error getting leader state for topic/partition: ", topic.Name, partition.ID)
		leader = -1
	}

	stats.Leader = leader

	stats.Replicas = partition.Replicas

	isr, err := partition.ISR()

	if err != nil {
		log.Errorln("Error getting replica state for topic/partition: ", topic.Name, partition.ID)
		isr = nil
	}

	stats.ISR = isr
	underReplicated, err := partition.UnderReplicated()

	if err != nil {
		log.Errorln("Error getting replica state for topic/partition: ", topic.Name, partition.ID)
		underReplicated = false
	}

	stats.Underrepplicated = underReplicated

	stats.Unavailable = false

	stats.ZkResponseTime = time.Since(zkStartTime)

	if (len(stats.Replicas) < 2) && (!intInSlice(stats.Leader, brokerIds)) {
		log.Infoln(fmt.Sprintf("Ignore reading offsets from dead broker ["+strconv.Itoa(int(stats.Leader))+"] for topic, partition: ", topic.Name, partition.ID, err))
		stats.Offset = -1
		stats.OldOffset = -1
		stats.Unavailable = true
	} else {
		startTime := time.Now()
		currentOffset, err := c.brokerClient.GetOffset(topic.Name, partition.ID, sarama.OffsetNewest)
		oldestOffset, err2 := c.brokerClient.GetOffset(topic.Name, partition.ID, sarama.OffsetOldest)
		stats.ResponseTime = time.Since(startTime)
		if isdebug {
			log.Infoln(fmt.Sprintf("===Finish reading offset for topic, partition from leader: ", topic.Name, partition.ID, stats.Leader, time.Since(startTime)))
		}

		if err != nil || err2 != nil {
			log.Errorln(fmt.Sprintf("Error reading offsets from broker for topic, partition: ", topic.Name, partition.ID, err))
			c.Lock()
			c.kafkaMetrics["broker_not_response"].With(prometheus.Labels{"cluster": c.cluster, "broker": strconv.Itoa(int(leader))}).Set(float64(1))
			c.Unlock()
			stats.Unavailable = true
			//c.initClients()
			return &stats, nil
		}
		stats.Offset = currentOffset
		stats.OldOffset = oldestOffset
	}

	return &stats, nil
}

func (c *collector) updateMetrics() {
	startTime := time.Now()
	log.Infoln("Updating metrics, Time: ", time.Now())

	//monitor dead brokers
	log.Infoln("Looking for dead brokers")

	c.deadBrokers = nil       //reset deadbroker list
	c.deadPartitionList = nil //reset unavailable partition list
	curController, err := c.zkClient.Controller()

	if err != nil {
		c.initClients()
		return
	}

	controllerhost, _ := c.zkClient.Broker(strconv.Itoa(int(curController)))

	seedBrokers, err := c.zkClient.BrokerList()
	if err != nil {
		log.Errorln("Error reading brokers from zk", err)
		return
		//panic(err)
	}

	log.Infoln("Current online brokers:")
	log.Infoln(seedBrokers)

	c.Lock()
	c.kafkaMetrics["cluster_current_nodes"].With(prometheus.Labels{"cluster": c.cluster}).Set(float64(len(seedBrokers)))
	c.kafkaMetrics["cluster_current_controller"].With(prometheus.Labels{"cluster": c.cluster, "broker": controllerhost}).Set(float64(curController))
	c.kafkaMetrics["cluster_nodes"].With(prometheus.Labels{"cluster": c.cluster}).Set(float64(len(c.brokerList)))
	c.Unlock()
	for _, seed_broker := range c.brokerList {
		if (!stringInSlice(seed_broker, seedBrokers)) {
			c.deadBrokers = append(c.deadBrokers, seed_broker)
			c.Lock()
			c.kafkaMetrics["cluster_dead_broker"].With(prometheus.Labels{"cluster": c.cluster, "broker": seed_broker}).Set(1)
			c.Unlock()
		}
	}
	log.Infoln("Current dead brokers:")
	log.Infoln(c.deadBrokers)
	log.Infoln("Finish hunting for dead brokers")

	oldConsumerGroups, err := c.zkClient.Consumergroups()
	log.Infoln("Num of consumergroup: " + strconv.Itoa(len(oldConsumerGroups)))

	if err != nil {
		log.Errorln("Error reading consumergroup offsets: ", err)
		return
	}

	topics, err := c.zkClient.Topics()

	if err != nil {
		c.initClients()
		return
	}
	var wg2 sync.WaitGroup
	ch := make(chan *PartitionStats, 6000)

	for _, topic := range topics {
		if c.topicsFilter != "" {
			match, err := regexp.MatchString(c.topicsFilter, topic.Name)

			if err != nil {
				log.Errorln("Invalid Regex: ", err)
				panic("Exiting..")
			}

			if !match {
				log.Infoln("Filtering out " + topic.Name)
				continue
			}
		}

		wg2.Add(1)

		go func(topic *kazoo.Topic, chMain chan<- *PartitionStats, wgMain *sync.WaitGroup, metrics map[string]*prometheus.GaugeVec) {
			defer wgMain.Done()
			partitions, _ := topic.Partitions()
			for _, partition := range partitions {
				wgMain.Add(1)
				go c.getPartitionStats(topic, partition, chMain, wgMain, metrics, oldConsumerGroups)
			}

		}(topic, ch, &wg2, c.kafkaMetrics)
	}
	wg2.Wait()

	log.Infoln("Done updating metrics in: ", time.Since(startTime))
	close(ch)
}

func (c *collector) updateTopics() {
	startTime := time.Now()
	log.Infoln("Updating  topics stats, Time: ", time.Now())
	c.brokerClient.Topics()
	log.Infoln("Done updating topics stats in: ", time.Since(startTime))

}

func (c *collector) resetMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, m := range c.kafkaMetrics {
		m.Reset()
	}
}

func (c *collector) collectMetrics(metrics chan<- prometheus.Metric) {
	c.RLock()
	defer c.RUnlock()
	for _, m := range c.kafkaMetrics {
		m.Collect(metrics)
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func intInSlice(x int32, a []int32) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
