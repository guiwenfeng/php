<?php
/**
 * Rsyslog 客户端
 * User: gwf
 * Date: 2019/4/25
 * Time: 9:39
 */

class Rsyslog {

    /**
     * Facility（日志类型）
     */
    const LOG_LOCAL0 = 16;
    const LOG_LOCAL1 = 17;
    const LOG_LOCAL2 = 18;
    const LOG_LOCAL3 = 19;
    const LOG_LOCAL4 = 20;
    const LOG_LOCAL5 = 21;
    const LOG_LOCAL6 = 22;
    const LOG_LOCAL7 = 23;

    /**
     * Severity（日志级别）
     */
    const LOG_EMERG   = 0;
    const LOG_ALERT   = 1;
    const LOG_CRIT    = 2;
    const LOG_ERR     = 3;
    const LOG_WARNING = 4;
    const LOG_NOTICE  = 5;
    const LOG_INFO    = 6;
    const LOG_DEBUG   = 7;

    /**
     * 主机地址
     * @var string
     */
    protected $host = '';

    /**
     * 主机端口
     * @var int
     */
    protected $port = 514;

    public function __construct() {

        $this->host = '';
        $this->port = 514;
    }

    /**
     * @param int $facility
     * @param int $severity
     * @param string $msg 消息
     * @param string $prog 进程名称
     * @param int $pid 进程ID
     */
    public function send($facility, $severity, $msg = '', $prog = '', $pid = 0) {

        $pri = "<" . (($facility * 8) + $severity) . ">";
        $header = date('M d H:i:s') . ' ' . gethostname();
        $tag = !empty($pid) ? $prog . '['. $pid . ']' : $prog;

        $syslogMsg =  $pri . $header . ' ' . $tag .  ': ' . $msg;

        $socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
        socket_sendto($socket, $syslogMsg, strlen($syslogMsg), 0, $this->host, $this->port);
        socket_close($socket);
    }
}