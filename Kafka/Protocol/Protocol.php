<?php
/**
 * 协议基类
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午10:34
 */

namespace Kafka\Protocol;

class Protocol {

    /**
     * Api Keys
     */
    const PRODUCE_REQUEST           = 0;  // Produce
    const FETCH_REQUEST             = 1;  // Fetch
    const OFFSET_REQUEST            = 2;  // ListOffsets
    const METADATA_REQUEST          = 3;  // Metadata
    const OFFSET_COMMIT_REQUEST     = 8;  // OffsetCommit
    const OFFSET_FETCH_REQUEST      = 9;  // OffsetFetch
    const DESCRIBE_GROUPS_REQUEST   = 15; // DescribeGroups
    const LIST_GROUPS_REQUEST       = 16; // ListGroups

    /**
     * API version
     */
    const API_VERSION_0 = 0;
    const API_VERSION_1 = 1;
    const API_VERSION_2 = 2;

    /**
     * 压缩算法
     */
    const COMPRESSION_NONE = 0; // no compression
    const COMPRESSION_GZIP = 1; // gzip
    const COMPRESSION_SNAPPY = 2; // snappy

    const DEFAULT_BROKER_VERION = '0.9.0.0';
    protected $version = self::DEFAULT_BROKER_VERION;

    /**
     * pack/unpack bit
     */
    const BIT_B64 = 'J'; // unsigned long long (big endian)
    const BIT_B32 = 'N'; // unsigned long (big endian)
    const BIT_B16 = 'n'; // unsigned short (big endian)
    const BIT_B16_SIGNED = 's'; // signed short (machine)
    const BIT_B8  = 'C'; // unsigned char

    private static $isLittleEndianSystem = null;

    /**
     * 请求头(api_key api_version correlation_id client_id)
     *
     * api_key => INT16
     * api_version => INT16
     * correlation_id => INT32
     * client_id => NULLABLE_STRING
     *
     * @param $clientId
     * @param $correlationId
     * @param $apiKey
     * @return string
     */
    public function requestHeader($clientId, $correlationId, $apiKey) {

        $binData  = self::pack(self::BIT_B16, $apiKey);
        $binData .= self::pack(self::BIT_B16, $this->getApiVersion($apiKey));
        $binData .= self::pack(self::BIT_B32, $correlationId);

        $binData .= self::encodeString($clientId, self::BIT_B16);

        return $binData;
    }

    /**
     * encode string
     *
     * @param $string
     * @param $type
     * @param $compression
     * @return string
     */
    public static function encodeString($string, $type, $compression = self::COMPRESSION_NONE) {

        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $string = \gzencode($string);
                break;
            case self::COMPRESSION_SNAPPY:
                // todo
                throw new \Exception('SNAPPY compression not yet implemented');
            default:
                throw new \Exception('Unknown compression flag: ' . $compression);
        }

        return self::pack($type, strlen($string)) . $string;
    }

    /**
     * decode string
     *
     * STRING: Represents a sequence of characters.
     * First the length N is given as an INT16.
     * Then N bytes follow which are the UTF-8 encoding of the character sequence.
     *
     * BYTES: Represents a raw sequence of bytes.
     * First the length N is given as an INT32. Then N bytes follow.
     *
     * NULLABLE_STRING:
     * A null value is encoded with length of -1 and there are no following bytes.
     *
     * NULLABLE_BYTES:
     * A null value is encoded with length of -1 and there are no following bytes.
     *
     * @param $data
     * @param $type
     * @param $compression
     * @return array
     */
    public function decodeString($data, $type, $compression = self::COMPRESSION_NONE) {

        $offset = ($type == self::BIT_B32) ? 4 : 2;
        $packLen = self::unpack($type, substr($data, 0, $offset));
        if ($packLen == 4294967295) { // uint32(4294967295) is int32 (-1)
            $packLen = 0;
        }

        if ($packLen == 0) {
            return [
                'length' => $offset,
                'data' => ''
            ];
        }

        $data = substr($data, $offset, $packLen);
        $offset += $packLen;

        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $data = \gzdecode($data);
                break;
            case self::COMPRESSION_SNAPPY:
                // todo
                throw new \Exception('SNAPPY compression not yet implemented');
            default:
                throw new \Exception('Unknown compression flag: ' . $compression);
        }

        return [
            'length' => $offset,
            'data' => $data
        ];
    }

    /**
     * encode array
     *
     * @param $func
     * @param array $array
     * @param null $options
     * @return string
     */
    public static function encodeArray($func, array $array, $options = null) {

        if (!is_callable($func, false)) {
            throw new \Exception('Encode array failed, given function is not callable.');
        }

        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            if (!is_null($options)) {
                $body .= call_user_func($func, $value, $options);
            } else {
                $body .= call_user_func($func, $value);
            }
        }

        return self::pack(self::BIT_B32, $arrayCount) . $body;
    }

    /**
     * decode array
     *
     * @param $func
     * @param $data
     * @param null $options
     * @return array
     */
    public function decodeArray($func, $data, $options = null) {

        if (!is_callable($func, false)) {
            throw new \Exception('Decode array failed, given function is not callable.');
        }

        $offset = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $result = [];
        for ($i = 0; $i < $arrayCount; $i++) {
            $value = substr($data, $offset);
            if (!is_null($options)) {
                $ret = call_user_func($func, $value, $options);
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

        return [
            'length' => $offset,
            'data' => $result
        ];
    }

    /**
     * decode primitive array
     *
     * @param $type
     * @param $data
     * @return array
     */
    public function decodePrimitiveArray($type, $data) {

        $offset = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        if ($arrayCount == 4294967295) { // uint32(4294967295) is int32 (-1)
            $arrayCount = 0;
        }

        $result = [];
        for ($i = 0; $i < $arrayCount; $i++) {
            if ($type == self::BIT_B64) {
                $result[] = self::unpack($type, substr($data, $offset, 8));
                $offset += 8;
            } elseif ($type == self::BIT_B32) {
                $result[] = self::unpack($type, substr($data, $offset, 4));
                $offset += 4;
            } elseif (in_array($type, array(self::BIT_B16, self::BIT_B16_SIGNED))) {
                $result[] = self::unpack($type, substr($data, $offset, 2));
                $offset += 2;
            } elseif ($type == self::BIT_B8) {
                $result[] = self::unpack($type, substr($data, $offset, 1));
                $offset += 1;
            }
        }

        return [
            'length' => $offset,
            'data' => $result
        ];
    }

    public static function pack($type, $data) {

        $data = pack($type, $data);

        return $data;
    }

    public static function unpack($type, $bytes) {
        
        if ($type == self::BIT_B16_SIGNED) {
            // According to PHP docs: 's' = signed short (always 16 bit, machine byte order)
            // So lets unpack it..
            $set = unpack($type, $bytes);

            // But if our system is little endian
            if (self::isSystemLittleEndian()) {
                // We need to flip the endianess because coming from kafka it is big endian
                $set = self::convertSignedShortFromLittleEndianToBigEndian($set);
            }
            $result = $set;
        } else {
            $result = unpack($type, $bytes);
        }

        return is_array($result) ? array_shift($result) : $result;
    }

    /**
     * 根据 broker version 指定 api version
     *
     * @param int $apikey API key
     * @return int
     */
    public function getApiVersion($apikey) {

        switch ($apikey) {
            case self::METADATA_REQUEST:
                return self::API_VERSION_0;
            case self::PRODUCE_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION_2;
                } elseif (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION_1;
                } else {
                    return self::API_VERSION_0;
                }
            case self::FETCH_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION_2;
                } elseif (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION_1;
                } else {
                    return self::API_VERSION_0;
                }
            case self::OFFSET_REQUEST:
                return self::API_VERSION_0;
            case self::OFFSET_COMMIT_REQUEST:
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION_2;
                } elseif (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION_1;
                } else {
                    return self::API_VERSION_0;
                }
            case self::OFFSET_FETCH_REQUEST:
                // There is no format difference between Offset Fetch Request v0 and v1.
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION_1; // Offset Fetch Request v1 will fetch offset from Kafka
                } else {
                    return self::API_VERSION_0; // Offset Fetch Request v0 will fetch offset from zookeeper
                }
        }

        // default
        return self::API_VERSION_0;
    }

    /**
     * 系统是否是小端字节序
     *
     * @return bool|null
     */
    public static function isSystemLittleEndian() {

        // If we don't know if our system is big endian or not yet...
        if (is_null(self::$isLittleEndianSystem)) {
            // Lets find out
            list($endiantest) = array_values(unpack('L', pack('V', 1)));
            if ($endiantest != 1) {
                // This is a big endian system
                self::$isLittleEndianSystem = false;
            } else {
                // This is a little endian system
                self::$isLittleEndianSystem = true;
            }
        }

        return self::$isLittleEndianSystem;
    }

    /**
     * Converts a signed short (16 bits) from little endian to big endian.
     *
     * @param $bits
     * @return mixed
     */
    public static function convertSignedShortFromLittleEndianToBigEndian($bits) {

        foreach ($bits as $index => $bit) {

            // get LSB
            $lsb = $bit & 0xff;

            // get MSB
            $msb = $bit >> 8 & 0xff;

            // swap bytes
            $bit = $lsb <<8 | $msb;

            if ($bit >= 32768) {
                $bit -= 65536;
            }
            $bits[$index] = $bit;
        }
        return $bits;
    }
}