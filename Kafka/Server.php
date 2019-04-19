<?php
/**
 * 连接到 kafka server
 * User: guiwenfeng
 * Date: 2019/4/4
 * Time: 下午6:41
 */

namespace Kafka;

class Server {

    use SingletonTrait;

    const READ_MAX_LEN = 5242880;
    const MAX_WRITE_BUFFER = 2048;

    private $stream = null;

    /**
     * @param string $host 主机地址
     * @param int $port 端口号
     */
    public function init($host, $port) {

        // 这里可以通过 config 来做
        $this->host = $host;
        $this->port = $port;
    }

    /**
     *
     */
    public function connect() {

        if (is_resource($this->stream)) {
            return;
        }

        if (empty($this->host)) {
            throw new \Exception('Cannot open null host.');
        }
        if ($this->port <= 0) {
            throw new \Exception('Cannot open without port.');
        }

        $this->stream = @fsockopen($this->host, $this->port, $errno, $errstr, 1);

        if ($this->stream == false) {
            $error = 'Could not connect to '
                . $this->host . ':' . $this->port
                . ' ('.$errstr.' ['.$errno.'])';
            throw new \Exception($error);
        }

        stream_set_blocking($this->stream, 0);
    }

    public function read($len, $verifyExactLength = false)
    {
        if ($len > self::READ_MAX_LEN) {
            throw new \Exception('Could not read '.$len.' bytes from stream, length too longer.');
        }

        $null = null;
        $read = array($this->stream);
        $readable = @stream_select($read, $null, $null, 1);
        if ($readable > 0) {
            $remainingBytes = $len;
            $data = $chunk = '';
            while ($remainingBytes > 0) {
                $chunk = fread($this->stream, $remainingBytes);
                if ($chunk === false) {
                    $this->close();
                    throw new \Exception('Could not read '.$len.' bytes from stream (no data)');
                }
                if (strlen($chunk) === 0) {
                    // Zero bytes because of EOF?
                    if (feof($this->stream)) {
                        $this->close();
                        throw new \Exception('Unexpected EOF while reading '.$len.' bytes from stream (no data)');
                    }
                    // Otherwise wait for bytes
                    $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
                    if ($readable !== 1) {
                        throw new \Exception('Timed out reading socket while reading ' . $len . ' bytes with ' . $remainingBytes . ' bytes to go');
                    }
                    continue; // attempt another read
                }
                $data .= $chunk;
                $remainingBytes -= strlen($chunk);
            }
            if ($len === $remainingBytes || ($verifyExactLength && $len !== strlen($data))) {
                // couldn't read anything at all OR reached EOF sooner than expected
                $this->close();
                throw new \Exception('Read ' . strlen($data) . ' bytes instead of the requested ' . $len . ' bytes');
            }

            return $data;
        }
        if (false !== $readable) {
            $res = stream_get_meta_data($this->stream);
            if (!empty($res['timed_out'])) {
                $this->close();
                throw new \Kafka\Exception('Timed out reading '.$len.' bytes from stream');
            }
        }
        $this->close();
        throw new \Kafka\Exception('Could not read '.$len.' bytes from stream (not readable)');
    }

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     *
     * @return integer
     * @throws \Kafka\Exception
     */
    public function write($buf)
    {
        $null = null;
        $write = array($this->stream);

        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $failedWriteAttempts = 0;
        $written = 0;
        $buflen = strlen($buf);
        while ($written < $buflen) {
            // wait for stream to become available for writing
            $writable = stream_select($null, $write, $null, 1);
            if ($writable > 0) {
                if ($buflen - $written > self::MAX_WRITE_BUFFER) {
                    // write max buffer size
                    $wrote = fwrite($this->stream, substr($buf, $written, self::MAX_WRITE_BUFFER));
                } else {
                    // write remaining buffer bytes to stream
                    $wrote = fwrite($this->stream, substr($buf, $written));
                }
                if ($wrote === -1 || $wrote === false) {
                    throw new \Exception('Could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $written . ' bytes');
                } elseif ($wrote === 0) {
                    // Increment the number of times we have failed
                    $failedWriteAttempts++;
                    if ($failedWriteAttempts > $this->maxWriteAttempts) {
                        throw new \Exception('After ' . $failedWriteAttempts . ' attempts could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $written . ' bytes');
                    }
                } else {
                    // If we wrote something, reset our failed attempt counter
                    $failedWriteAttempts = 0;
                }
                $written += $wrote;
                continue;
            }
            if (false !== $writable) {
                $res = stream_get_meta_data($this->stream);
                if (!empty($res['timed_out'])) {
                    throw new \Exception('Timed out writing ' . strlen($buf) . ' bytes to stream after writing ' . $written . ' bytes');
                }
            }
            throw new \Exception('Could not write ' . strlen($buf) . ' bytes to stream');
        }
        return $written;
    }
}