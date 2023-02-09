<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        //return Command::SUCCESS;
        $conf = new \RdKafka\Conf();
        /*
        $conf->set('bootstrap.servers', 'pkc-4r297.europe-west1.gcp.confluent.cloud:9092');
        $conf->set('security.protocol', 'SASL_SSL');
        $conf->set('sasl.mechanism', 'PLAIN');
        $conf->set('sasl.username', '4Y6GPCCOY2RMNUMX');
        $conf->set('sasl.password', 'GZOQwL1ahD6yd81uSRW+rfM++mx6BCoV61HqJDdWjjgi9nzoEWgYi5NB8mm6KkyH');
        $conf->set('group.id', 'myGroup');
        $conf->set('auto.offset.reset', 'earliest');
        */

        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('sasl.mechanism', $config['sasl_mechanism']);
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);

        $producer = new \RdKafka\Producer($conf);


        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');


        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }

}