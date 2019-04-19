<?php
/**
 * OffsetCommit 协议
 * User: guiwenfeng
 * Date: 2019/4/17
 * Time: 下午6:10
 */

namespace Kafka\Protocol;

class OffsetCommit extends Protocol {

    /**
     * OffsetCommit Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        if (!isset($payloads['group_id'])) {
            throw new \Exception('given offset commit data invalid. `group_id` is undefined.');
        }

        if (!isset($payloads['data'])) {
            throw new \Exception('given offset commit data invalid. `data` is undefined.');
        }

        if (!isset($payloads['generation_id'])) {
            $payloads['generation_id'] = -1;
        }

        if (!isset($payloads['member_id'])) {
            $payloads['member_id'] = '';
        }

        if (!isset($payloads['retention_time'])) {
            $payloads['retention_time'] = -1;
        }

        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);

        $header = $this->requestHeader('kafka-php', self::OFFSET_COMMIT_REQUEST, self::OFFSET_COMMIT_REQUEST);

        $data = self::encodeString($payloads['group_id'], self::BIT_B16);
        if ($version == self::API_VERSION_1) {
            $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::BIT_B16);
        }
        if ($version == self::API_VERSION_2) {
            $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::BIT_B16);
            $data .= self::pack(self::BIT_B64, $payloads['retention_time']);
        }

        $data .= self::encodeArray(array($this, 'encodeTopic'), $payloads['data']);
        $data  = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * OffsetCommit Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $topics = $this->decodeArray(array($this, 'decodeTopic'), substr($data, $offset));
        $offset += $topics['length'];

        return $topics['data'];
    }

    /**
     * encode topic
     *
     * @param $values
     * @return string
     */
    protected function encodeTopic($values) {

        if (!isset($values['topic_name'])) {
            throw new \Exception('given offset commit data invalid. `topic_name` is undefined.');
        }
        if (!isset($values['partitions'])) {
            throw new \Exception('given offset commit data invalid. `partitions` is undefined.');
        }

        $data  = self::encodeString($values['topic_name'], self::BIT_B16);
        $data .= self::encodeArray(array($this, 'encodePartition'), $values['partitions']);

        return $data;
    }

    /**
     * encode partition
     *
     * @param $values
     * @return string
     */
    protected function encodePartition($values) {

        if (!isset($values['partition'])) {
            throw new \Exception('given offset commit data invalid. `partition` is undefined.');
        }
        if (!isset($values['offset'])) {
            throw new \Exception('given offset commit data invalid. `offset` is undefined.');
        }
        if (!isset($values['metadata'])) {
            $values['metadata'] = '';
        }
        if (!isset($values['timestamp'])) {
            $values['timestamp'] = time() * 1000;
        }
        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);

        $data  = self::pack(self::BIT_B32, $values['partition']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        if ($version == self::API_VERSION_1) {
            $data .= self::pack(self::BIT_B64, $values['timestamp']);
        }
        $data .= self::encodeString($values['metadata'], self::BIT_B16);

        return $data;
    }

    /**
     * decode topic
     *
     * @param $data
     * @return array
     */
    protected function decodeTopic($data) {

        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];

        $partitions = $this->decodeArray(array($this, 'decodePartition'), substr($data, $offset));
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
    protected function decodePartition($data) {

        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;

        return [
            'length' => $offset,
            'data' => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
            ]
        ];
    }
}