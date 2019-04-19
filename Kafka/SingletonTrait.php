<?php
/**
 * 单例模式 trait
 * User: guiwenfeng
 * Date: 2019/4/8
 * Time: 下午10:11
 */

namespace Kafka;

trait SingletonTrait {

    private static $instance = null;

    public static function getInstance() {

        if (!(self::$instance instanceof self)) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    // 防止通过 new 创建对象
    private function __construct() {
    }

    // 防止通过 clone 创建对象
    private function __clone() {
    }
}