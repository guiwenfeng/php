<?php
/**
 * ListOffsets 协议
 * User: guiwenfeng
 * Date: 2019/4/10
 * Time: 下午5:16
 */

namespace Kafka\Protocol;

class ListOffsets extends Protocol {

    /**
     * ListOffsets Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        if (!isset($payloads['data'])) {
            throw new \Exception('given ListOffsets data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        $header = $this->requestHeader('kafka-php', self::OFFSET_REQUEST, self::OFFSET_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::encodeArray(array($this, 'encodeOffsetTopic'), $payloads['data']);
        $data   = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * ListOffsets Response Decode
     *
     * @param $data
     * @return mixed
     */
    public function decode($data) {

        $offset = 0;

        $version = $this->getApiVersion(self::OFFSET_REQUEST);
        $topics = $this->decodeArray(array($this, 'decodeOffsetTopic'), substr($data, $offset), $version);
        $offset += $topics['length'];

        return $topics['data'];
    }

    /**
     * encode offset topic
     *
     * @param $values
     * @return string
     */
    protected function encodeOffsetTopic($values) {

        if (!isset($values['topic_name'])) {
            throw new \Exception('given offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Exception('given offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::BIT_B16);
        $partitions = self::encodeArray(array($this, 'encodeOffsetPartition'), $values['partitions']);

        return $topic . $partitions;
    }

    /**
     * encode offset partition
     *
     * @param $values
     * @return string
     */
    protected function encodeOffsetPartition($values) {

        if (!isset($values['partition_id'])) {
            throw new \Exception('given offset data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['time'])) {
            $values['time'] = -1; // -1
        }

        if (!isset($values['max_offset'])) {
            $values['max_offset'] = 100000;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['time']);

        if ($this->getApiVersion(self::OFFSET_REQUEST) == self::API_VERSION_0) {
            $data .= self::pack(self::BIT_B32, $values['max_offset']);
        }

        return $data;
    }

    /**
     * decode offset topic
     *
     * @param $data
     * @param $version
     * @return array
     */
    protected function decodeOffsetTopic($data, $version) {

        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];
        $partitions = $this->decodeArray(array($this, 'decodeOffsetPartition'), substr($data, $offset), $version);
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
     * decode offset partition
     *
     * @param $data
     * @param $version
     * @return array
     */
    protected function decodeOffsetPartition($data, $version) {

        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $timestamp = 0;
        if ($version != self::API_VERSION_0) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }
        $offsets = $this->decodePrimitiveArray(self::BIT_B64, substr($data, $offset));
        $offset += $offsets['length'];

        return [
            'length' => $offset,
            'data' => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'timestamp' => $timestamp,
                'offsets' => $offsets['data'],
            ]
        ];
    }
}