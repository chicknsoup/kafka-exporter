# Kafka Exporter

Just another simple kafka exporter to scrape metrics from zookeeper and kafka brokers

# Configuration

A config file must be present in conf folder with the name equal to -cluster.name parameter value (For example, if -cluster.name=lab --> config file should be conf/lab.conf)

Config content:
    
```
{
"brokers": ["10.0.0.1:9290","10.0.0.2:9290","10.0.0.3:9290"]
}
```

# Installation 

./kafka-exporter -zk.servers=<list of comma separated http:/server:ports> -cluster.name=<name of cluster>
   
# Notes
```
-enable.consumer.metrics=true - Scrape consumer metrics (disable this if the cluster is too big with so many consumers)
-isdebug - Log debug     
```
# Metrics
<pre>
topic_current_offset
topic_oldest_offset
cluster_nodes
cluster_current_nodes
topic_partition_current_isr
topic_partition_current_offset
topic_partition_oldest_offset
topic_partition_current_replicas
topic_partition_current_leader
cluster_current_controller
under_replicated_partition
consumer_group_partition_offset
consumer_group_partition_lag
topic_unavailable_partition
cluster_dead_broker
brokers_monitor_fails
</pre>