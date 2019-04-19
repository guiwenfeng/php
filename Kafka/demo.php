<?php
/**
 * Created by PhpStorm.
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午10:10
 */

require './vendor/autoload.php';

//$producer = new \Kafka\Producer();
//$result = $producer->send([
//    [
//        'topic' => 'test',
//        'patition' => 0,
//        'key' => 'test',
//        'value' => 'test',
//    ],
//    [
//        'topic' => 'hello',
//        'value' => 'world',
//    ],
//]);

$consumer = new \Kafka\Consumer();

// 获取消息
$result = $consumer->fetch([
    [
        'topic' => 'test',
        'offset' => 1
    ]
]);

// 获取 topic offset
//$consumer->offset([
//    [
//        'topic' => 'test',
//    ]
//]);

// 消费组列表
//$consumer->groupList();

// 消费组详情
//$consumer->groupDetail([
//    'groupId' => [
//        'console-consumer-67034'
//    ]
//]);

//$consumer->offsetCommit([]);

//$consumer->offsetFetch([]);