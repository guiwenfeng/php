<?php
/**
 * Metadata 协议
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午10:19
 */

namespace Kafka\Protocol;

class Metadata extends Protocol {

    /**
     * Metadata Request Encode
     *
     * @param $topics
     * @return string
     */
    public function encode($topics) {

        if (!is_array($topics)) {
            $topics = [$topics];
        }

        foreach ($topics as $topic) {
            if (!is_string($topic)) {
                throw new \Exception('metadata topic name have invalid value.');
            }
        }

        $header = $this->requestHeader('kafka-php', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray(array($this, 'encodeString'), $topics, self::BIT_B16);
        $data   = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * Metadata Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $brokersRet = $this->decodeArray(array($this, 'metaBroker'), substr($data, $offset));
        $offset += $brokersRet['length'];
        $topicsRet = $this->decodeArray(array($this, 'topicMetaData'), substr($data, $offset));
        $offset += $topicsRet['length'];

        return [
            'brokers' => $brokersRet['data'],
            'topics'  => $topicsRet['data'],
        ];
    }

    /**
     * decode broker metadata
     *
     * @param $data
     * @return array
     */
    protected function metaBroker($data) {

        $offset = 0;
        $nodeId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $hostNameInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $hostNameInfo['length'];
        $port = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        return [
            'length' => $offset,
            'data' => [
                'nodeId' => $nodeId,
                'host' => $hostNameInfo['data'],
                'port' => $port
            ]
        ];
    }

    /**
     * decode topic metadata
     *
     * @param $data
     * @return array
     */
    protected function topicMetaData($data) {

        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];
        $partitionsRet = $this->decodeArray(array($this, 'partitionMetaData'), substr($data, $offset));
        $offset += $partitionsRet['length'];

        return [
            'length' => $offset,
            'data' => [
                'errorCode' => $errorCode,
                'topic' => $topicInfo['data'],
                'partitions' => $partitionsRet['data'],
            ]
        ];
    }

    /**
     * decode partition metadata
     *
     * @param $data
     * @return array
     */
    protected function partitionMetaData($data) {

        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partition = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $leader = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $replicas = $this->decodePrimitiveArray(self::BIT_B32, substr($data, $offset));
        $offset += $replicas['length'];
        $isr = $this->decodePrimitiveArray(self::BIT_B32, substr($data, $offset));
        $offset += $isr['length'];

        return [
            'length' => $offset,
            'data' => [
                'errorCode' => $errorCode,
                'partition' => $partition,
                'leader' => $leader,
                'replicas' => $replicas['data'],
                'isr' => $isr['data'],
            ]
        ];
    }
}