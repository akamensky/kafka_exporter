package main

import "github.com/akamensky/promexp"

type metric struct {
	name    string
	desc    string
	labels  []string
	enabled bool
}

func (m *metric) set(value float64, labels ...string) {
	if !m.enabled {
		return
	}

	if len(labels) != len(m.labels) {
		panic("mismatch in label count for metric")
	}

	l := make(map[string]string)
	for i, label := range m.labels {
		l[label] = labels[i]
	}

	exporter.SetGauge(m.name, value, m.desc, l)
}

var (
	exporter    = promexp.NewExporter()
	metricsData = make([]byte, 0)
	allMetrics  = make([]*metric, 0)

	clusterBrokers                     = newMetric("kafka_brokers", "Number of Brokers in the Kafka Cluster.")
	clusterBrokerInfo                  = newMetric("kafka_broker_info", "Information about the Kafka Broker.", "address", "id")
	topicPartitions                    = newMetric("kafka_topic_partitions", "Number of partitions for this Topic", "topic")
	topicCurrentOffset                 = newMetric("kafka_topic_partition_current_offset", "Current Offset of a Broker at Topic/Partition", "topic", "partition")
	topicOldestOffset                  = newMetric("kafka_topic_partition_oldest_offset", "Oldest Offset of a Broker at Topic/Partition", "topic", "partition")
	topicPartitionLeader               = newMetric("kafka_topic_partition_leader", "Leader Broker ID of this Topic/Partition", "topic", "partition")
	topicPartitionReplicas             = newMetric("kafka_topic_partition_replicas", "Number of Replicas for this Topic/Partition", "topic", "partition")
	topicPartitionInSyncReplicas       = newMetric("kafka_topic_partition_in_sync_replica", "Number of In-Sync Replicas for this Topic/Partition", "topic", "partition")
	topicPartitionUsesPreferredReplica = newMetric("kafka_topic_partition_leader_is_preferred", "1 if Topic/Partition is using the Preferred Broker", "topic", "partition")
	topicUnderReplicatedPartition      = newMetric("kafka_topic_partition_under_replicated_partition", "1 if Topic/Partition is under Replicated", "topic", "partition")
	consumergroupMembers               = newMetric("kafka_consumergroup_members", "Amount of members in a consumer group", "consumergroup")
	consumergroupCurrentOffset         = newMetric("kafka_consumergroup_current_offset", "Current Offset of a ConsumerGroup at Topic/Partition", "consumergroup", "topic", "partition")
	consumergroupLag                   = newMetric("kafka_consumergroup_lag", "Current Approximate Lag of a ConsumerGroup at Topic/Partition", "consumergroup", "topic", "partition")
	consumergroupCurrentOffsetSum      = newMetric("kafka_consumergroup_current_offset_sum", "Current Offset of a ConsumerGroup at Topic for all partitions", "consumergroup", "topic")
	consumergroupLagSum                = newMetric("kafka_consumergroup_lag_sum", "Current Approximate Lag of a ConsumerGroup at Topic for all partitions", "consumergroup", "topic")
)

func newMetric(name, desc string, labels ...string) *metric {
	m := &metric{
		name:    name,
		desc:    desc,
		labels:  labels,
		enabled: true,
	}
	allMetrics = append(allMetrics, m)
	return m
}

func disableMetric(name string) {
	for _, m := range allMetrics {
		if m.name == name {
			m.enabled = false
		}
	}
}

func allMetricNames() []string {
	result := make([]string, 0)

	for _, m := range allMetrics {
		result = append(result, m.name)
	}

	return result
}
