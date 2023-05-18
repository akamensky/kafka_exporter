package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/akamensky/argparse"
	"os"
	"strings"
	"time"
)

type config struct {
	http  httpConfig
	kafka kafkaConfig
}

type httpConfig struct {
	addr    string
	context string

	tlsEnable        bool
	tlsCaFile        string
	tlsCertFile      string
	tlsKeyFile       string
	tlsUseMutualAuth bool
}

type kafkaConfig struct {
	servers                 []string
	version                 sarama.KafkaVersion
	metadataRefreshInterval time.Duration
	workers                 int

	saslEnable          bool
	saslHandshake       bool
	saslUsername        string
	saslPassword        string
	saslMechanism       string
	saslServiceName     string
	saslKrbConfig       string
	saslKrbRealm        string
	saslKrbAuthType     string
	saslKrbKeytabPath   string
	saslDisablePaFxFast bool

	tlsEnable             bool
	tlsCaFile             string
	tlsCertFile           string
	tlsKeyFile            string
	tlsServerName         string
	tlsInsecureSkipVerify bool
}

func parseConfig() (*config, error) {
	p := argparse.NewParser("kafka_exporter", "Prometheus exporter for Kafka")

	httpListenArg := p.String("", "http.addr", &argparse.Options{
		Help:    "Address to listen for Prometheus scraper",
		Default: ":9308",
	})

	httpContextArg := p.String("", "http.context", &argparse.Options{
		Help:    "Path to expose for metrics",
		Default: "/metrics",
	})

	httpTlsEnableArg := p.Flag("", "http.tls", &argparse.Options{
		Help:    "Use TLS for Prometheus endpoint",
		Default: false,
	})

	httpTlsCaFileArg := p.String("", "http.tls.ca", &argparse.Options{
		Help:    "Path to CA file to use for Prometheus endpoint",
		Default: "",
	})

	httpTlsCertFileArg := p.String("", "http.tls.cert", &argparse.Options{
		Help:    "Path to certificate file to use for Prometheus endpoint",
		Default: "",
	})

	httpTlsKeyFileArg := p.String("", "http.tls.key", &argparse.Options{
		Help:    "Path to key file to use for Prometheus endpoint",
		Default: "",
	})

	httpTlsUseMutualAuthArg := p.Flag("", "http.tls.use.mutual.auth", &argparse.Options{
		Help:    "Use TLS mutual authentication for Protmetheus endpoint",
		Default: false,
	})

	kafkaServersArg := p.String("", "kafka.servers", &argparse.Options{
		Help:    "Comma-separated list of Kafka bootstrap servers",
		Default: "localhost:9092",
	})

	kafkaVersionArg := p.String("", "kafka.version", &argparse.Options{
		Help:    "Minimum Kafka client API verion",
		Default: sarama.V2_0_0_0.String(),
	})

	kafkaDisableMetricsArg := p.String("", "kafka.disable.metrics", &argparse.Options{
		Help:    fmt.Sprintf("Comma-separated list of metric names to skip, possible values are: %s", strings.Join(allMetricNames(), ", ")),
		Default: "",
	})

	kafkaRefreshMetadataArg := p.String("", "kafka.refresh.metadata", &argparse.Options{
		Help:    "Interval to refresh Kafka metadata",
		Default: "60s",
	})

	kafkaWorkersArg := p.Int("", "kafka.workers", &argparse.Options{
		Help:    "Number of workers to use for reading data from Kafka",
		Default: 100,
	})

	kafkaSaslEnableArg := p.Flag("", "kafka.sasl", &argparse.Options{
		Help:    "Use SASL authentication for Kafka client",
		Default: false,
	})

	kafkaSaslHandshakeArg := p.Flag("", "kafka.sasl.handshake", &argparse.Options{
		Help:    "Only set this flag if using SASL proxy that does not behave like Kafka interface",
		Default: true,
	})

	kafkaSaslUsernameArg := p.String("", "kafka.sasl.username", &argparse.Options{
		Help:    "SASL username",
		Default: "",
	})

	kafkaSaslPasswordArg := p.String("", "kafka.sasl.password", &argparse.Options{
		Help:    "SASL password",
		Default: "",
	})

	kafkaSaslMechanismArg := p.Selector("", "kafka.sasl.mechanism", []string{"plain", "scram-sha256", "scram-sha512", "gssapi"}, &argparse.Options{
		Help:    "SASL SCRAM algo",
		Default: "plain",
	})

	kafkaSaslServiceNameArg := p.String("", "kafka.sasl.service.name", &argparse.Options{
		Help:    "Kerberos service name",
		Default: "",
	})

	kafkaSaslKrbConfigPathArg := p.String("", "kafka.sasl.krb.config.path", &argparse.Options{
		Help:    "Kerberos config path",
		Default: "",
	})

	kafkaSaslKrbRealmArg := p.String("", "kafka.sasl.krb.realm", &argparse.Options{
		Help:    "Kerberos realm",
		Default: "",
	})

	kafkaSaslKrbAuthTypeArg := p.Selector("", "kafka.sasl.krb.auth.type", []string{"keytab", "user"}, &argparse.Options{
		Help:    "Kerberos auth type",
		Default: "keytab",
	})

	kafkaSaslKrbKeytabPathArg := p.String("", "kafka.sasl.krb.keytab.path", &argparse.Options{
		Help:    "Path to Kerberos keytab file",
		Default: "keytab",
	})

	kafkaSaslDisablePaFxFastArg := p.Flag("", "kafka.sasl.disable.pa_fx_fast", &argparse.Options{
		Help:    "Configure Kerberos to not use PA_FX_FAST",
		Default: false,
	})

	kafkaTlsEnableArg := p.Flag("", "kafka.tls", &argparse.Options{
		Help:    "Use TLS for Kafka connection",
		Default: false,
	})

	kafkaTlsCaFileArg := p.String("", "kafka.tls.ca", &argparse.Options{
		Help:    "Path to CA file",
		Default: "",
	})

	kafkaTlsCertFileArg := p.String("", "kafka.tls.cert", &argparse.Options{
		Help:    "Path to certificate file",
		Default: "",
	})

	kafkaTlsKeyFileArg := p.String("", "kafka.tls.key", &argparse.Options{
		Help:    "Path to key file",
		Default: "",
	})

	kafkaTlsServerNameArg := p.String("", "kafka.tls.server.name", &argparse.Options{
		Help:    "Server name used to verify TLS certificate returned from Kafka. Must be matching that on certificate configured in Kafka unless --tls.insecure.skip.tls.verify is provided",
		Default: "",
	})

	kafkaTlsInsecureSkipVerifyArg := p.Flag("", "kafka.tls.insecure.skip.verify", &argparse.Options{
		Help:    "If provided will skip validation of TLS certificate returned from Kafka",
		Default: false,
	})

	if err := p.Parse(os.Args); err != nil {
		return nil, err
	}

	v, err := sarama.ParseKafkaVersion(*kafkaVersionArg)
	if err != nil {
		return nil, err
	}

	d, err := time.ParseDuration(*kafkaRefreshMetadataArg)
	if err != nil {
		return nil, err
	}

	if *kafkaDisableMetricsArg != "" {
		for _, name := range strings.Split(*kafkaDisableMetricsArg, ",") {
			disableMetric(name)
		}
	}

	conf := &config{
		http: httpConfig{
			addr:             *httpListenArg,
			context:          *httpContextArg,
			tlsEnable:        *httpTlsEnableArg,
			tlsCaFile:        *httpTlsCaFileArg,
			tlsCertFile:      *httpTlsCertFileArg,
			tlsKeyFile:       *httpTlsKeyFileArg,
			tlsUseMutualAuth: *httpTlsUseMutualAuthArg,
		},
		kafka: kafkaConfig{
			servers:                 strings.Split(*kafkaServersArg, ","),
			version:                 v,
			metadataRefreshInterval: d,
			workers:                 *kafkaWorkersArg,
			saslEnable:              *kafkaSaslEnableArg,
			saslHandshake:           *kafkaSaslHandshakeArg,
			saslUsername:            *kafkaSaslUsernameArg,
			saslPassword:            *kafkaSaslPasswordArg,
			saslMechanism:           *kafkaSaslMechanismArg,
			saslServiceName:         *kafkaSaslServiceNameArg,
			saslKrbConfig:           *kafkaSaslKrbConfigPathArg,
			saslKrbRealm:            *kafkaSaslKrbRealmArg,
			saslKrbAuthType:         *kafkaSaslKrbAuthTypeArg,
			saslKrbKeytabPath:       *kafkaSaslKrbKeytabPathArg,
			saslDisablePaFxFast:     *kafkaSaslDisablePaFxFastArg,
			tlsEnable:               *kafkaTlsEnableArg,
			tlsCaFile:               *kafkaTlsCaFileArg,
			tlsCertFile:             *kafkaTlsCertFileArg,
			tlsKeyFile:              *kafkaTlsKeyFileArg,
			tlsServerName:           *kafkaTlsServerNameArg,
			tlsInsecureSkipVerify:   *kafkaTlsInsecureSkipVerifyArg,
		},
	}

	return conf, nil
}
