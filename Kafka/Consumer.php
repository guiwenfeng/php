<?php
/**
 * 消费者
 * User: guiwenfeng
 * Date: 2019/4/11
 * Time: 下午3:18
 */

namespace Kafka;

class Consumer {

    private $server = null;

    /**
     * Consumer constructor.
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
     * 获取消息
     */
    public function fetch($data) {

        $sendData = $this->convertMessage($data);

        foreach ($sendData as $brokerId => $topicList) {

            $params = [
                'max_wait_time' => 5000,
                'min_bytes' => '100',
                'data' => $topicList,
            ];
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::FETCH_REQUEST, $params);
            $this->server->write($requestData);

            $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
            $data = $this->server->read($dataLen);

            $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $result = \Kafka\Protocol::decode(\Kafka\Protocol::FETCH_REQUEST, substr($data, 4));

            echo json_encode($result);exit;
        }
    }

    /**
     * 获取 topic offset
     */
    public function offset($data) {

        $sendData = $this->convertMessage($data);

        foreach ($sendData as $brokerId => $topicList) {

            $params = [
                'data' => $topicList,
            ];
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_REQUEST, $params);
            $this->server->write($requestData);

            $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
            $data = $this->server->read($dataLen);

            $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_REQUEST, substr($data, 4));

            echo json_encode($result);exit;
        }
    }

    /**
     * 消费组列表
     */
    public function groupList() {

        $params = [];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::LIST_GROUPS_REQUEST, $params);
        $this->server->write($requestData);

        $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
        $data = $this->server->read($dataLen);

        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result = \Kafka\Protocol::decode(\Kafka\Protocol::LIST_GROUPS_REQUEST, substr($data, 4));

        echo json_encode($result);exit;
    }

    /**
     * 消费组详情
     */
    public function groupDetail($data) {

        if (!isset($data['groupId'])) {
            throw new \Exception('given group data invalid. `groupId` is undefined.');
        }

        $params = $data['groupId'];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::DESCRIBE_GROUPS_REQUEST, $params);
        $this->server->write($requestData);

        $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
        $data = $this->server->read($dataLen);

        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result = \Kafka\Protocol::decode(\Kafka\Protocol::DESCRIBE_GROUPS_REQUEST, substr($data, 4));

        echo json_encode($result);exit;
    }

    public function offsetCommit($data) {

        $params = [
            'group_id' => '',
            'member_id' => '',
            'generation_id' => 1,
            'data' => [
                [
                    'topic_name' => '__consumer_offsets',
                    'partitions' => [
                        [
                            'partition' => 0,
                            'offset' => 2
                        ]
                    ]
                ]
            ]
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, $params);
        $this->server->write($requestData);

        $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
        $data = $this->server->read($dataLen);

        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, substr($data, 4));

        echo json_encode($result);exit;
    }

    public function offsetFetch($data) {

        $params = [
            'group_id' => '',
            'data' => [
                [
                    'topic_name' => '__consumer_offsets',
                    'partitions' => [
                        [
                            'partition' => 0,
                        ]
                    ]
                ]
            ]
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, $params);
        $this->server->write($requestData);

        $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $this->server->read(4));
        $data = $this->server->read($dataLen);

        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));

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

            $partition[$value['topic']] = [
                'partition_id' => $partitionId,
                'offset' => $value['offset']
            ];

            $topicData['topic_name'] = $value['topic'];
            $topicData['partitions'][$partitionId] = $partition[$value['topic']];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }
}