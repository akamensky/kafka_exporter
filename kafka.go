package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"golang.org/x/exp/slog"
	"os"
	"strings"
	"sync"
	"time"
)

type kafka struct {
	conf                kafkaConfig
	client              sarama.Client
	mutex               sync.Mutex
	nextMetadataRefresh time.Time
}

func newKafka(conf kafkaConfig) (*kafka, error) {
	c := sarama.NewConfig()
	c.ClientID = "kafka_exporter"
	c.Version = conf.version

	if conf.saslEnable {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		conf.saslMechanism = strings.ToLower(conf.saslMechanism)
		switch conf.saslMechanism {
		case "scram-sha512":
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		case "scram-sha256":
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "gssapi":
			c.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeGSSAPI)
			c.Net.SASL.GSSAPI.ServiceName = conf.saslServiceName
			c.Net.SASL.GSSAPI.KerberosConfigPath = conf.saslKrbConfig
			c.Net.SASL.GSSAPI.Realm = conf.saslKrbRealm
			c.Net.SASL.GSSAPI.Username = conf.saslUsername
			if conf.saslKrbAuthType == "keytab" {
				c.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
				c.Net.SASL.GSSAPI.KeyTabPath = conf.saslKrbKeytabPath
			} else {
				c.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
				c.Net.SASL.GSSAPI.Password = conf.saslPassword
			}
			if conf.saslDisablePaFxFast {
				c.Net.SASL.GSSAPI.DisablePAFXFAST = true
			}
		case "plain":
		default:
			return nil, fmt.Errorf(`invalid sasl mechanism "%s": can only be "scram-sha256", "scram-sha512", "gssapi" or "plain"`, conf.saslMechanism)
		}

		c.Net.SASL.Enable = true
		c.Net.SASL.Handshake = conf.saslHandshake

		if conf.saslUsername != "" {
			c.Net.SASL.User = conf.saslUsername
		}

		if conf.saslPassword != "" {
			c.Net.SASL.Password = conf.saslPassword
		}
	}

	if conf.tlsEnable {
		c.Net.TLS.Enable = true

		c.Net.TLS.Config = &tls.Config{
			ServerName:         conf.tlsServerName,
			InsecureSkipVerify: conf.tlsInsecureSkipVerify,
		}

		if conf.tlsCaFile != "" {
			if ca, err := os.ReadFile(conf.tlsCaFile); err == nil {
				c.Net.TLS.Config.RootCAs = x509.NewCertPool()
				c.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
		}

		ok, err := canReadCertAndKey(conf.tlsCertFile, conf.tlsKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "error reading cert and key")
		}
		if ok {
			cert, err := tls.LoadX509KeyPair(conf.tlsCertFile, conf.tlsKeyFile)
			if err == nil {
				c.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				return nil, err
			}
		}
	}

	c.Metadata.RefreshFrequency = conf.metadataRefreshInterval

	c.Metadata.AllowAutoTopicCreation = false

	client, err := sarama.NewClient(conf.servers, c)

	if err != nil {
		return nil, errors.Wrap(err, "Error while setting up Kafka client")
	}

	k := &kafka{
		conf:                conf,
		client:              client,
		mutex:               sync.Mutex{},
		nextMetadataRefresh: time.Now(),
	}

	return k, nil
}

func (k *kafka) collect() {
	var wg = sync.WaitGroup{}
	clusterBrokers.set(float64(len(k.client.Brokers())))
	for _, b := range k.client.Brokers() {
		go clusterBrokerInfo.set(1, b.Addr(), fmt.Sprint(b.ID()))
	}

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(k.nextMetadataRefresh) {
		slog.Debug("Refreshing client metadata")

		if err := k.client.RefreshMetadata(); err != nil {
			slog.Error("Cannot refresh topics, using cached data", slog.String("error", err.Error()))
		}

		k.nextMetadataRefresh = now.Add(k.conf.metadataRefreshInterval)
	}

	topics, err := k.client.Topics()
	if err != nil {
		slog.Error("Cannot get topics", slog.String("error", err.Error()))
		return
	}

	topicChannel := make(chan string)

	getTopicMetrics := func(topic string) {
		defer wg.Done()

		partitions, err := k.client.Partitions(topic)
		if err != nil {
			slog.Error("Cannot get partitions", slog.String("topic", topic), slog.String("error", err.Error()))
			return
		}

		go topicPartitions.set(float64(len(partitions)), topic)

		k.mutex.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		k.mutex.Unlock()

		for _, partition := range partitions {
			if topicPartitionLeader.enabled {
				broker, err := k.client.Leader(topic, partition)
				if err != nil {
					slog.Error("Cannot get leader", slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
				} else {
					go topicPartitionLeader.set(float64(broker.ID()), topic, fmt.Sprint(partition))
				}
			}

			if topicCurrentOffset.enabled || consumergroupLag.enabled {
				currentOffset, err := k.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					slog.Error("Cannot get current offset", slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
				} else {
					k.mutex.Lock()
					offset[topic][partition] = currentOffset
					k.mutex.Unlock()

					go topicCurrentOffset.set(float64(currentOffset), topic, fmt.Sprint(partition))
				}
			}

			if topicOldestOffset.enabled {
				oldestOffset, err := k.client.GetOffset(topic, partition, sarama.OffsetOldest)
				if err != nil {
					slog.Error("Cannot get oldest offset", slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
				} else {
					go topicOldestOffset.set(float64(oldestOffset), topic, fmt.Sprint(partition))
				}
			}

			if topicPartitionReplicas.enabled {
				replicas, err := k.client.Replicas(topic, partition)
				if err != nil {
					slog.Error("Cannot get replicas", slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
				} else {
					go topicPartitionReplicas.set(float64(len(replicas)), topic, fmt.Sprint(partition))
				}
			}

			if topicUnderReplicatedPartition.enabled {
				inSyncReplicas, err := k.client.InSyncReplicas(topic, partition)
				if err != nil {
					slog.Error("Cannot get in-sync replicas", slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
				} else {
					go topicPartitionInSyncReplicas.set(float64(len(inSyncReplicas)), topic, fmt.Sprint(partition))
				}
			}
		}
	}

	loopTopics := func() {
		ok := true
		for ok {
			topic, open := <-topicChannel
			ok = open
			if open {
				getTopicMetrics(topic)
			}
		}
	}

	minx := func(x int, y int) int {
		if x < y {
			return x
		} else {
			return y
		}
	}

	N := len(topics)
	if N > 1 {
		N = minx(N/2, k.conf.workers)
	}

	for w := 1; w <= N; w++ {
		go loopTopics()
	}

	for _, topic := range topics {
		wg.Add(1)
		topicChannel <- topic
	}
	close(topicChannel)

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()

		if err := broker.Open(k.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			slog.Error("Cannot connect to broker", slog.String("broker.id", fmt.Sprint(broker.ID())), slog.String("error", err.Error()))
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			slog.Error("Cannot get consumer groups from broker", slog.String("broker.id", fmt.Sprint(broker.ID())), slog.String("error", err.Error()))
			return
		}

		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			groupIds = append(groupIds, groupId)
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			slog.Error("Cannot describe consumer groups", slog.String("broker.id", fmt.Sprint(broker.ID())), slog.String("error", err.Error()))
			return
		}
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			for topic, partitions := range offset {
				for partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}

			go consumergroupMembers.set(float64(len(group.Members)), group.GroupId)

			if consumergroupCurrentOffset.enabled || consumergroupLag.enabled {
				offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
				if err != nil {
					slog.Error("Cannot get offset of consumer group", slog.String("consumergroup", group.GroupId), slog.String("error", err.Error()))
					continue
				}

				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if !topicConsumed {
						continue
					}

					for partition, offsetFetchResponseBlock := range partitions {
						err := offsetFetchResponseBlock.Err
						if err != sarama.ErrNoError {
							slog.Error("Error fetching consumer offsets for partition", slog.String("partition", fmt.Sprint(partition)), slog.String("error", err.Error()))
							continue
						}
						currentOffset := offsetFetchResponseBlock.Offset

						go consumergroupCurrentOffset.set(float64(currentOffset), group.GroupId, topic, fmt.Sprint(partition))

						k.mutex.Lock()
						if offset, ok := offset[topic][partition]; ok {
							// If the topic is consumed by that consumer group, but no offset associated with the partition
							// forcing lag to -1 to be able to alert on that
							var lag int64
							if offsetFetchResponseBlock.Offset == -1 {
								lag = -1
							} else {
								lag = offset - offsetFetchResponseBlock.Offset
								if lag < 0 {
									lag = 0
								}
							}

							go consumergroupLag.set(float64(lag), group.GroupId, topic, fmt.Sprint(partition))
						} else {
							slog.Error("Cannot get consumer group lag", slog.String("consumergroup", group.GroupId), slog.String("topic", topic), slog.String("partition", fmt.Sprint(partition)))
						}
						k.mutex.Unlock()
					}
				}
			}
		}
	}

	slog.Debug("Fetching consumer group metrics")
	if len(k.client.Brokers()) > 0 {
		for _, broker := range k.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
		}
		wg.Wait()
	} else {
		slog.Error("No valid broker, cannot get consumer group metrics")
	}
}
