<?php
/**
 * ListGroups 协议
 * User: guiwenfeng
 * Date: 2019/4/17
 * Time: 上午10:22
 */

namespace Kafka\Protocol;

class ListGroups extends Protocol {

    /**
     * ListGroups Request Encode
     *
     * @param $payloads
     * @return string
     */
    public function encode($payloads) {

        $header = $this->requestHeader('kafka-php', self::LIST_GROUPS_REQUEST, self::LIST_GROUPS_REQUEST);
        $data = self::encodeString($header, self::BIT_B32);

        return $data;
    }

    /**
     * ListGroups Response Decode
     *
     * @param $data
     * @return array
     */
    public function decode($data) {

        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $groups = $this->decodeArray(array($this, 'listGroup'), substr($data, $offset));

        return [
            'errorCode' => $errorCode,
            'groups' => $groups['data'],
        ];
    }

    /**
     * decode list group
     *
     * @param $data
     * @return array
     */
    protected function listGroup($data) {

        $offset = 0;
        $groupId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $groupId['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocolType['length'];

        return [
            'length' => $offset,
            'data' => [
                'groupId' => $groupId['data'],
                'protocolType' => $protocolType['data'],
            ]
        ];
    }
}