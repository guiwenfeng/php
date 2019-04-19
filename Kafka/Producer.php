<?php
/**
 * 生产者
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午5:39
 */

namespace Kafka;

class Producer {

    private $server = null;

    /**
     * Producer constructor.
     */
    public function __construct() {

        $host = '127.0.0.1';
        $port = 9092;
        $this->server = \Kafka\Server::getInstance();
        $this->server->init($host,$port);
        $this->server->connect();

        $params = [];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
        $this->server->write($requestData);

        $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
        $data = $this->server->read($dataLen);

        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
//        echo json_encode($result);exit;

        $broker = \Kafka\Broker::getInstance();
        $broker->setData($result['brokers'], $result['topics']);
    }

    /**
     * 发送消息
     */
    public function send($data) {

        $sendData = $this->convertMessage($data);

        $RequiredAcks = 1;
        foreach ($sendData as $brokerId => $topicList) {

            $params = [
                'required_ack' => $RequiredAcks,
                'timeout' => 1000,
                'data' => $topicList,
            ];
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            $this->server->write($requestData);

            if ($RequiredAcks != 0) { // If it is 0 the server will not send any response
                $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
                $data = $this->server->read($dataLen);

                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $ret = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
                $result[] = $ret;
            }
        }

        echo json_encode($result);exit;
    }

    protected function convertMessage($data) {

        $broker = \Kafka\Broker::getInstance();
        $topicsInfo = $broker->getTopics();

        $sendData = [];
        foreach ($data as $value) {

            if (!isset($value['topic']) || !trim($value['topic'])) {
                continue;
            }

            if (!isset($topicsInfo[$value['topic']])) {
                continue;
            }

            if (!isset($value['value']) || !trim($value['value'])) {
                continue;
            }

            if (!isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicsInfo[$value['topic']];
            $partitionsMeta = array_keys($topicMeta);
            shuffle($partitionsMeta);
            $partitionId = 0;
            if (!isset($value['partition']) || !isset($topicMeta[$value['partition']])) {
                $partitionId = $partitionsMeta[0];
            } else {
                $partitionId = $value['partition'];
            }

            $brokerId = $topicMeta[$partitionId];

            $partition[$value['topic']]['partition_id'] = $partitionId;
            if (trim($value['key']) != '') {
                $partition[$value['topic']]['messages'][] = ['value' => $value['value'], 'key' => $value['key']];
            } else {
                $partition[$value['topic']]['messages'][] = $value['value'];
            }

            $topicData['topic_name'] = $value['topic'];
            $topicData['partitions'][$partitionId] = $partition[$value['topic']];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }
}