<?php
/**
 * OffsetFetch 协议
 * User: guiwenfeng
 * Date: 2019/4/18
 * Time: 上午11:38
 */

namespace Kafka\Protocol;

class OffsetFetch extends Protocol {

    /**
     * OffsetFetch Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {//var_dump($payloads);exit;

        if (!isset($payloads['data'])) {
            throw new \Exception('given offset fetch data invalid. `data` is undefined.');
        }

        if (!isset($payloads['group_id'])) {
            throw new \Exception('given offset fetch data invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::OFFSET_FETCH_REQUEST, self::OFFSET_FETCH_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::BIT_B16);
        $data  .= self::encodeArray(array($this, 'encodeOffsetTopic'), $payloads['data']);
        $data   = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * OffsetFetch Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $topics = $this->decodeArray(array($this, 'offsetTopic'), substr($data, $offset));
        $offset += $topics['length'];

        return $topics['data'];
    }



    /**
     * encode topic
     *
     * @param $values
     * @return string
     */
    protected function encodeOffsetTopic($values) {

        if (!isset($values['topic_name'])) {
            throw new \Exception('given offset fetch data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Exception('given offset fetch data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::BIT_B16);
        $partitions = self::encodeArray(array($this, 'encodeOffsetPartition'), $values['partitions']);

        return $topic . $partitions;
    }

    /**
     * encode partition
     *
     * @param $values
     * @return string
     */
    protected function encodeOffsetPartition($values) {

        if (!isset($values['partition'])) {
            throw new \Exception('given offset fetch data invalid. `partition` is undefined.');
        }

        return self::pack(self::BIT_B32, $values['partition']);
    }

    /**
     * decode topic
     *
     * @param $data
     * @return array
     */
    protected function offsetTopic($data) {

        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];

        $partitions = $this->decodeArray(array($this, 'offsetPartition'), substr($data, $offset));
        $offset += $partitions['length'];

        return [
            'length' => $offset,
            'data' => [
                'topic' => $topicInfo['data'],
                'partitions'  => $partitions['data'],
            ]
        ];
    }

    /**
     * decode partition
     *
     * @param $data
     * @return array
     */
    protected function offsetPartition($data) {

        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $roffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;

        $metadata = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $metadata['length'];
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;

        return [
            'length' => $offset,
            'data' => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'metadata' => $metadata['data'],
                'offset' => $roffset,
            ]
        ];
    }
}