<?php
    include "Redis.php";
    echo "<pre>";
    echo "command   type           Res";echo "<br />";

    $redis = new Redis();
    $redis->connect('192.168.16.2');

    $res = $redis->ping();
    echo "PING:     Simple Strings ".$res;echo "<br />";
    $res = $redis->echo('hello world!');
    echo "ECHO:     Bulk Strings   ".$res;echo "<br />";
    $res = $redis->flushdb();
    echo "FLUSHDB:  Simple Strings ".$res;echo "<br />";
    $res = $redis->flushall();
    echo "FLUSHALL: Simple Strings ".$res;echo "<br />";
    $res = $redis->set('hello', 'world');
    echo "SET:      Simple Strings ".$res;echo "<br />";
    $res = $redis->keys('*');
    echo "KEYS:     Arrays         ".json_encode($res);echo "<br />";
    $res = $redis->scan('0');
    echo "SCAN:     Arrays         ".json_encode($res);echo "<br />";
    $res = $redis->get('hello');
    echo "GET:      Bulk Strings   ".$res;echo "<br />";
    $res = $redis->del('hello');
    echo "DEL:      Integers       ".$res;echo "<br />";

    echo "</pre>";
