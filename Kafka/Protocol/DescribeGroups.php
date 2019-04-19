<?php
/**
 * DescribeGroups 协议
 * User: guiwenfeng
 * Date: 2019/4/17
 * Time: 上午10:52
 */

namespace Kafka\Protocol;

class DescribeGroups extends Protocol {

    /**
     * DescribeGroups Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        if (!is_array($payloads)) {
            $payloads = [$payloads];
        }

        $header = $this->requestHeader('kafka-php', self::DESCRIBE_GROUPS_REQUEST, self::DESCRIBE_GROUPS_REQUEST);
        $data = self::encodeArray(array($this, 'encodeString'), $payloads, self::BIT_B16);
        $data = self::encodeString($header . $data, self::BIT_B32);

        return $data;
    }

    /**
     * DescribeGroups Response Decode
     *
     * @param $data
     * @return mixed
     */
    public function decode($data) {

        $offset = 0;
        $groups = $this->decodeArray(array($this, 'describeGroup'), substr($data, $offset));
        $offset += $groups['length'];

        return $groups['data'];
    }

    /**
     * decode group
     *
     * @param $data
     * @return array
     */
    protected function describeGroup($data) {

        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $groupId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $groupId['length'];
        $state = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $state['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocolType['length'];
        $protocol = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocol['length'];

        $members = $this->decodeArray(array($this, 'groupMember'), substr($data, $offset));
        $offset += $members['length'];

        return [
            'length' => $offset,
            'data' => [
                'errorCode' => $errorCode,
                'groupId' => $groupId['data'],
                'state' => $state['data'],
                'protocolType' => $protocolType['data'],
                'protocol' => $protocol['data'],
                'members' => $members['data']
            ]
        ];
    }

    /**
     * decode group member
     *
     * @param $data
     * @return array
     */
    protected function groupMember($data) {

        $offset = 0;
        $memberId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $memberId['length'];
        $clientId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $clientId['length'];
        $clientHost = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $clientHost['length'];
        $metadata = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $metadata['length'];
        $assignment = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $assignment['length'];

        $metaData = $metadata['data'];
        $metaOffset = 0;
        $metaVersion = self::unpack(self::BIT_B16, substr($metaData, $metaOffset, 2));
        $metaOffset += 2;
        $metaTopics = $this->decodeArray(array($this, 'decodeString'), substr($metaData, $metaOffset), self::BIT_B16);
        $metaOffset += $metaTopics['length'];
        $metaUserData = $this->decodeString(substr($metaData, $metaOffset), self::BIT_B32);

        $assignmentData = $assignment['data'];
        $assignmentOffset = 0;
        $assignmentVersion = self::unpack(self::BIT_B16_SIGNED, substr($assignmentData, $assignmentOffset, 2));
        $assignmentOffset += 2;
        $assignmentPartition = $this->decodeArray(array($this, 'assignmentPartition'),
            substr($assignmentData, $assignmentOffset));
        $assignmentOffset += $assignmentPartition['length'];
        $assignmentUserData = $this->decodeString(substr($assignmentData, $assignmentOffset), self::BIT_B32);




        return [
            'length' => $offset,
            'data' => [
                'memberId' => $memberId['data'],
                'clientId' => $clientId['data'],
                'clientHost' => $clientHost['data'],
                'metadata' => [
                    'version' => $metaVersion,
                    'topics'  => $metaTopics['data'],
                    'userData' => $metaUserData['data'],
                ],
                'assignment' => [
                    'version' => $assignmentVersion,
                    'partitions' => $assignmentPartition['data'],
                    'userData' => $assignmentUserData['data']
                ]
            ]
        ];
    }

    /**
     * decode assignment partition
     *
     * @param $data
     * @return array
     */
    protected function assignmentPartition($data) {

        $offset = 0;
        $topicName = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicName['length'];
        $partitions = $this->decodePrimitiveArray(self::BIT_B32, substr($data, $offset));
        $offset += $partitions['length'];

        return [
            'length' => $offset,
            'data' => [
                'topic' => $topicName['data'],
                'partitions' => $partitions['data'],
            ]
        ];
    }
}