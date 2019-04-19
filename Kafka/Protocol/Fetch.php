<?php
/**
 * Fetch 协议
 * User: guiwenfeng
 * Date: 2019/4/10
 * Time: 上午10:18
 */

namespace Kafka\Protocol;

class Fetch extends Protocol {

    /**
     * Fetch Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        if (!isset($payloads['data'])) {
            throw new \Exception('given fetch data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        if (!isset($payloads['max_wait_time'])) {
            $payloads['max_wait_time'] = 100; // default timeout 100ms
        }

        if (!isset($payloads['min_bytes'])) {
            $payloads['min_bytes'] = 64 * 1024; // 64k
        }

        $header = $this->requestHeader('kafka-php', self::FETCH_REQUEST, self::FETCH_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::pack(self::BIT_B32, $payloads['max_wait_time']);
        $data  .= self::pack(self::BIT_B32, $payloads['min_bytes']);
        $data  .= self::encodeArray(array($this, 'encodeFetchTopic'), $payloads['data']);
        $data   = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * Fetch Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $version = $this->getApiVersion(self::FETCH_REQUEST);
        $throttleTime = 0;
        if ($version != self::API_VERSION_0) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
        }

        $topics = $this->decodeArray(array($this, 'decodeFetchTopic'), substr($data, $offset));
        $offset += $topics['length'];

        return [
            'throttleTime' => $throttleTime,
            'topics' => $topics['data'],
        ];
    }

    /**
     * encode fetch topic
     *
     * @param $values
     * @return string
     */
    protected function encodeFetchTopic($values) {

        if (!isset($values['topic_name'])) {
            throw new \Exception('given fetch data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Exception('given fetch data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::BIT_B16);
        $partitions = self::encodeArray(array($this, 'encodeFetchPartition'), $values['partitions']);

        return $topic . $partitions;
    }

    /**
     * encode fetch partition
     *
     * @param $values
     * @return string
     */
    protected function encodeFetchPartition($values) {

        if (!isset($values['partition_id'])) {
            throw new \Exception('given fetch data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['offset'])) {
            $values['offset'] = 0;
        }

        if (!isset($values['max_bytes'])) {
            $values['max_bytes'] = 2 * 1024 * 1024;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        $data .= self::pack(self::BIT_B32, $values['max_bytes']);

        return $data;
    }

    /**
     * decode fetch topic
     *
     * @param $data
     * @return array
     */
    protected function decodeFetchTopic($data) {

        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];
        $partitions = $this->decodeArray(array($this, 'decodeFetchPartition'), substr($data, $offset));
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
     * decode fetch partition
     *
     * @param $data
     * @return array
     */
    protected function decodeFetchPartition($data) {

        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $highWaterMark = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;

        $messageSetSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        if ($offset < strlen($data) && $messageSetSize) {
            $messages = $this->decodeMessageSetArray(array($this, 'decodeMessageSet'), substr($data, $offset, $messageSetSize), $messageSetSize);
            $offset += $messages['length'];
        }

        return [
            'length' => $offset,
            'data' => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'highWaterMark' => $highWaterMark,
                'records' => isset($messages['data']) ? $messages['data'] : [],
            ]
        ];
    }

    /**
     * decode MessageSet array
     *
     * @param $func
     * @param $data
     * @param $messageSetSize
     * @return array
     */
    protected function decodeMessageSetArray($func, $data, $messageSetSize = null) {

        if (!is_callable($func, false)) {
            throw new \Exception('Decode array failed, given function is not callable.');
        }

        $offset = 0;

        $result = array();
        while ($offset < strlen($data)) {
            $value = substr($data, $offset);
            if (!is_null($messageSetSize)) {
                $ret = call_user_func($func, $value, $messageSetSize);
            } else {
                $ret = call_user_func($func, $value);
            }

            if (!is_array($ret) && $ret === false) {
                break;
            }

            if (!isset($ret['length']) || !isset($ret['data'])) {
                throw new \Exception('Decode array failed, given function return format is invliad');
            }
            if ($ret['length'] == 0) {
                continue;
            }

            $offset += $ret['length'];
            $result[] = $ret['data'];
        }

        if ($offset < $messageSetSize) {
            $offset = $messageSetSize;
        }

        return [
            'length' => $offset,
            'data' => $result
        ];
    }

    /**
     * decode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param $data
     * @return array
     */
    protected function decodeMessageSet($data) {

        if (strlen($data) <= 12) {
            return false;
        }

        $offset = 0;
        $msgOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;
        $messageSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $ret = $this->decodeMessage(substr($data, $offset, $messageSize), $messageSize);
        if (!is_array($ret) && $ret == false) {
            return false;
        }
        $offset += $ret['length'];

        return [
            'length' => $offset,
            'data' => [
                'offset' => $msgOffset,
                'size'   => $messageSize,
                'message' => $ret['data'],
            ]
        ];
    }

    /**
     * decode message
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param $data
     * @param $messageSize
     * @return array
     */
    protected function decodeMessage($data, $messageSize) {

        if (strlen($data) < $messageSize || !$messageSize) {
            return false;
        }

        $offset = 0;
        $crc = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $magic = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        $offset += 1;
        $attr  = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        $offset += 1;

        $timestamp = 0;
        $backOffset = $offset;
        try { // try unpack message format v0 and v1, if use v1 unpack fail, try unpack v0
            $version = $this->getApiVersion(self::FETCH_REQUEST);
            if ($version == self::API_VERSION_2) {
                $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
            }

            $keyRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset += $keyRet['length'];

            $valueRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset += $valueRet['length'];
            if ($offset != $messageSize) {
                throw new \Exception('pack message fail, message len:' . $messageSize . ' , data unpack offset :' . $offset);
            }
        } catch (\Exception $e) { // try unpack message format v0

            $timestamp = 0;
            $offset = $backOffset;

            $keyRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset += $keyRet['length'];

            $valueRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset += $valueRet['length'];
        }

        return [
            'length' => $offset,
            'data' => [
                'crc' => $crc,
                'magic' => $magic,
                'attr' => $attr,
                'timestamp' => $timestamp,
                'key' => $keyRet['data'],
                'value' => $valueRet['data'],
            ]
        ];
    }
}