<?php
if (!defined('CRLF')) {
    define('CRLF', sprintf('%s%s', chr(13), chr(10)));
}

class Redis
{

    protected $conn = NULL;

    protected $cmd = '';

    protected $response = '';

    public function connect($host = '127.0.0.1', $port = 6379, $timeout = 0)
    {
        $this->conn = stream_socket_client("tcp://$host:$port", $errno, $errstr, $timeout);

        if (!$this->conn) {
            die("ERROR: $errno - $errstr");
        }
    }

    public function __call($name, $args)
    {
        array_unshift($args, $name);
        return $this->exec($args);
    }

    public function exec($args)
    {
        $this->_makeCommand($args);
        fwrite($this->conn, $this->cmd, strlen($this->cmd));
        $this->_fetchResponse();
        $result = $this->_fmtResponse();

        return $result;
    }

    private function _fmtResponse()
    {
        if ($this->response[0] == '-') {
            $this->response = ltrim($this->response, '-');
            list($errStr, $this->response) = explode(CRLF, $this->response, 2);
            throw new \Exception($errStr, 500);
        }

        switch ($this->response[0]) {
            case '+':
            case ':':
                list($ret, $this->response) = explode(CRLF, $this->response, 2);
                $ret = substr($ret, 1);
                break;
            case '$':
                $this->response = ltrim($this->response, '$');
                list($slen, $this->response) = explode(CRLF, $this->response, 2);
                $ret = substr($this->response, 0, intval($slen));
                $this->response = substr($this->response, 2 + $slen);
                break;
            case '*':
                $this->response = ltrim($this->response, '*');
                list($count, $this->response) = explode(CRLF, $this->response, 2);
                for ($i = 0; $i < $count; $i++) {
                    $tmp = $this->_fmtResponse();
                    $ret[] = $tmp;
                }
                return $ret;
        }

        return $ret;
    }

    private function _fetchResponse()
    {
        $this->response = fread($this->conn, 4096);
        stream_set_blocking($this->conn, 0); // 设置连接为非阻塞

        while ($buf = fread($this->conn, 4096)) {
            $this->response .= $buf;
        }
        stream_set_blocking($this->conn, 1); // 恢复连接为阻塞
    }

    private function _makeCommand($args)
    {
        $this->cmd = sprintf('*%d%s%s%s', count($args), CRLF, implode(array_map(function ($arg) {
            return sprintf('$%d%s%s', strlen($arg), CRLF, $arg);
        }, $args), CRLF), CRLF);
    }
}
