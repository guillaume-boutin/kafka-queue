<?php

namespace Guillaume\KafkaQueue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $conf = new Conf();

        $conf->set("bootstrap.servers", $config["bootstrap_servers"]);
        $conf->set("security.protocol", $config["security_protocol"]);
        $conf->set("sasl.mechanism", $config["sasl_mechanism"]);
        $conf->set("sasl.username", $config["sasl_username"]);
        $conf->set("sasl.password", $config["sasl_password"]);

        $producer = new Producer($conf);

        $conf->set("group.id", $config["group_id"]);
        $conf->set("auto.offset.reset", "earliest");

        $consumer = new KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
