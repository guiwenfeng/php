<?php
/**
 * 通信协议
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午6:12
 */

namespace Kafka;

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
    const GROUP_COORDINATOR_REQUEST = 10; // FindCoordinator
    const JOIN_GROUP_REQUEST        = 11; // JoinGroup
    const HEART_BEAT_REQUEST        = 12; // Heartbeat
    const LEAVE_GROUP_REQUEST       = 13; // LeaveGroup
    const SYNC_GROUP_REQUEST        = 14; // SyncGroup
    const DESCRIBE_GROUPS_REQUEST   = 15; // DescribeGroups
    const LIST_GROUPS_REQUEST       = 16; // ListGroups

    const NO_ERROR = 0;
    const ERROR_UNKNOWN = -1;
    const OFFSET_OUT_OF_RANGE = 1;
    const INVALID_MESSAGE = 2;
    const UNKNOWN_TOPIC_OR_PARTITION = 3;
    const INVALID_MESSAGE_SIZE = 4;
    const LEADER_NOT_AVAILABLE = 5;
    const NOT_LEADER_FOR_PARTITION = 6;
    const REQUEST_TIMED_OUT = 7;
    const BROKER_NOT_AVAILABLE = 8;
    const REPLICA_NOT_AVAILABLE = 9;
    const MESSAGE_SIZE_TOO_LARGE = 10;
    const STALE_CONTROLLER_EPOCH = 11;
    const OFFSET_METADATA_TOO_LARGE = 12;
    const GROUP_LOAD_IN_PROGRESS = 14;
    const GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    const NOT_COORDINATOR_FOR_GROUP = 16;
    const INVALID_TOPIC = 17;
    const RECORD_LIST_TOO_LARGE = 18;
    const NOT_ENOUGH_REPLICAS = 19;
    const NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
    const INVALID_REQUIRED_ACKS = 21;
    const ILLEGAL_GENERATION = 22;
    const INCONSISTENT_GROUP_PROTOCOL = 23;
    const INVALID_GROUP_ID = 24;
    const UNKNOWN_MEMBER_ID = 25;
    const INVALID_SESSION_TIMEOUT = 26;
    const REBALANCE_IN_PROGRESS = 27;
    const INVALID_COMMIT_OFFSET_SIZE = 28;
    const TOPIC_AUTHORIZATION_FAILED = 29;
    const GROUP_AUTHORIZATION_FAILED = 30;
    const CLUSTER_AUTHORIZATION_FAILED = 31;
    const UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;

    private static $protocol = [
        \Kafka\Protocol::PRODUCE_REQUEST => 'Produce',
        \Kafka\Protocol::FETCH_REQUEST => 'Fetch',
        \Kafka\Protocol::OFFSET_REQUEST => 'ListOffsets',
        \Kafka\Protocol::METADATA_REQUEST => 'Metadata',
        \Kafka\Protocol::OFFSET_COMMIT_REQUEST => 'OffsetCommit',
        \Kafka\Protocol::OFFSET_FETCH_REQUEST => 'OffsetFetch',
        \Kafka\Protocol::DESCRIBE_GROUPS_REQUEST => 'DescribeGroups',
        \Kafka\Protocol::LIST_GROUPS_REQUEST => 'ListGroups',
    ];

    private static $object = null;

    public static function encode($key, $data) {

        if (!isset(self::$protocol[$key])) {
            throw new \Exception('Not support api key, key:' . $key);
        }

        $className = self::$protocol[$key];
        $class = '\\Kafka\\Protocol\\' . $className;

        if (!(self::$object instanceof $class)) {
            self::$object = new $class();
        }
        return self::$object->encode($data);
    }

    public static function decode($key, $data) {

        if (!isset(self::$protocol[$key])) {
            throw new \Kafka\Exception('Not support api key, key:' . $key);
        }

        $className = self::$protocol[$key];
        $class = '\\Kafka\\Protocol\\' . $className;

        if (!(self::$object instanceof $class)) {
            self::$object = new $class();
        }
        return self::$object->decode($data);
    }
}