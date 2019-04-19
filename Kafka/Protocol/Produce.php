<?php
/**
 * Produce 协议
 * User: guiwenfeng
 * Date: 2019/4/8
 * Time: 下午6:15
 */

namespace Kafka\Protocol;

class Produce extends Protocol {

    /**
     * magic value
     * In versions prior to Kafka 0.10, the only supported message format version (which is indicated in the magic value) was 0.
     * Message format version 1 was introduced with timestamp support in version 0.10.
     */
    const MESSAGE_MAGIC_VERSION_0 = 0;
    const MESSAGE_MAGIC_VERSION_1 = 1;

    /**
     * Produce Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        if (!isset($payloads['data'])) {
            throw new \Exception('given produce data invalid. `data` is undefined.');
        }

        if (!isset($payloads['required_ack'])) {
            // default server will not send any response
            $payloads['required_ack'] = 0;
        }

        if (!isset($payloads['timeout'])) {
            // default timeout 100ms
            $payloads['timeout'] = 100;
        }

        $header = $this->requestHeader('kafka-php', self::PRODUCE_REQUEST, self::PRODUCE_REQUEST);
        $data   = self::pack(self::BIT_B16, $payloads['required_ack']);
        $data  .= self::pack(self::BIT_B32, $payloads['timeout']);
        $data  .= self::encodeArray(array($this, 'encodeProduceTopic'), $payloads['data'], self::COMPRESSION_NONE);
        $data   = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * Produce Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $version = $this->getApiVersion(self::PRODUCE_REQUEST);
        $ret = $this->decodeArray(array($this, 'decodeProduceTopic'), substr($data, $offset), $version);
        $offset += $ret['length'];
        $throttleTime = 0;
        if ($version == self::API_VERSION_2) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        }

        return [
            'data' => $ret['data'],
            'throttleTime' => $throttleTime,
        ];
    }

    /**
     * encode produce topic
     *
     * @param $values
     * @param $compression
     * @return string
     */
    protected function encodeProduceTopic($values, $compression) {

        if (!isset($values['topic_name'])) {
            throw new \Exception('given produce data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Exception('given produce data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::BIT_B16);
        $partitions = self::encodeArray(array($this, 'encodeProducePartition'), $values['partitions'], $compression);

        return $topic . $partitions;
    }

    /**
     * encode produce partition
     *
     * @param $values
     * @param $compression
     * @return string
     */
    protected function encodeProducePartition($values, $compression) {

        if (!isset($values['partition_id'])) {
            throw new \Exception('given produce data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['messages']) || empty($values['messages'])) {
            throw new \Exception('given produce data invalid. `messages` is undefined.');
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::encodeString($this->encodeMessageSet($values['messages'], $compression), self::BIT_B32);

        return $data;
    }

    /**
     * encode messageSet
     *
     * @param $messages
     * @param $compression
     * @return string
     */
    protected function encodeMessageSet($messages, $compression) {

        if (!is_array($messages)) {
            $messages = [$messages];
        }

        $data = '';
        $next = 0;
        foreach ($messages as $message) {

            $tmpMessage = $this->encodeMessage($message, $compression);

            $data .= self::pack(self::BIT_B64, $next) . self::encodeString($tmpMessage, self::BIT_B32);
            $next++;
        }
        return $data;
    }

    /**
     * encode message
     *
     * @param $message
     * @param $compression
     * @return mixed|string
     */
    protected function encodeMessage($message, $compression) {

        // magic -> int8  attribute -> int8
        $version = $this->getApiVersion(self::PRODUCE_REQUEST);
        $magic = ($version != self::API_VERSION_2) ? self::MESSAGE_MAGIC_VERSION_0 : self::MESSAGE_MAGIC_VERSION_1;
        $data  = self::pack(self::BIT_B8, $magic);
        $data .= self::pack(self::BIT_B8, $compression);

        $key = '';
        if (is_array($message)) {
            $key = $message['key'];
            $message = $message['value'];
        }

        // message key
        $data .= self::encodeString($key, self::BIT_B32);
        // message value
        $data .= self::encodeString($message, self::BIT_B32, $compression);

        $crc = crc32($data);

        $message = self::pack(self::BIT_B32, $crc) . $data;

        return $message;
    }

    /**
     * decode produce topic
     *
     * @param $data
     * @param $version
     * @return array
     */
    protected function decodeProduceTopic($data, $version) {

        $offset = 0;
        $topicInfo = $this->decodeString($data, self::BIT_B16);
        $offset += $topicInfo['length'];
        $ret = $this->decodeArray(array($this, 'decodeProducePartition'), substr($data, $offset), $version);
        $offset += $ret['length'];

        return [
            'length' => $offset,
            'data' => [
                'topic' => $topicInfo['data'],
                'partitions'=> $ret['data'],
            ]
        ];
    }

    /**
     * decode produce partition
     *
     * @param $data
     * @param $version
     * @return array
     */
    protected function decodeProducePartition($data, $version) {

        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;
        $timestamp = 0;
        if ($version == self::API_VERSION_2) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }

        return [
            'length' => $offset,
            'data' => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'offset' => $partitionOffset,
                'timestamp' => $timestamp,
            ]
        ];
    }
}