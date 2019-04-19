<?php
/**
 * Broker
 * User: guiwenfeng
 * Date: 2019/4/9
 * Time: 上午9:58
 */

namespace Kafka;

class Broker {

    use SingletonTrait;

    private $brokers = [];

    private $topics = [];

    public function getBrokers() {

        return $this->brokers;
    }

    public function getTopics() {

        return $this->topics;
    }

    public function setData($brokersRes, $topicsRes) {

        $brokers = [];
        foreach ($brokersRes as $broker) {
            $nodeId = $broker['nodeId'];
            $hostname = $broker['host'] . ':' . $broker['port'];
            $brokers[$nodeId] = $hostname;
        }
        $this->brokers = $brokers;

        $topics = [];
        foreach ($topicsRes as $topic) {
            if ($topic['errorCode'] != \Kafka\Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . \Kafka\Protocol::getError($topic['errorCode']));
                continue;
            }

            $item = [];
            foreach ($topic['partitions']  as $partition) {
                $item[$partition['partition']] = $partition['leader'];
            }
            $topics[$topic['topic']] = $item;
        }
        $this->topics = $topics;
    }
}