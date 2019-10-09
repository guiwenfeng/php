<?php
/**
 * 对称加密
 * User: gwf
 * Date: 2019/10/9
 * Time: 15:02
 */

class Encryption {

    /**
     * Encryption cipher(加密算法)
     *
     * @var string
     */
    protected $cipher = 'aes-128';

    /**
     * Cipher mode(加密模式)
     *
     * @var string
     */
    protected $mode = 'cbc';

    /**
     * Cipher handle
     *
     * @var string
     */
    protected $handle = '';

    /**
     * Encryption key(密钥)
     *
     * @var string
     */
    protected $key;

    /**
     * Base64 encode(base64 编码)
     *
     * @var bool
     */
    protected $base64 = false;

    /**
     * Message authentication(消息认证)
     *
     * @var bool
     */
    protected $hmac = true;

    /**
     * Message authentication(消息认证算法)
     *
     * @var string
     */
    protected $hmac_digest = 'sha512';

    /**
     * List of available modes
     *
     * @var    array
     */
    protected $_modes = array(
        'cbc' => 'cbc',
        'ecb' => 'ecb',
        'ofb' => 'ofb',
        'cfb' => 'cfb',
        'cfb8' => 'cfb8',
        'ctr' => 'ctr',
    );

    /**
     * List of supported HMAC algorithms
     *
     * name => digest size pairs
     *
     * @var    array
     */
    protected $_digests = array(
        'sha224' => 28,
        'sha256' => 32,
        'sha384' => 48,
        'sha512' => 64
    );

    /**
     * Encryption constructor.
     *
     * @param array $params Configuration parameters
     * @return void
     *
     * Example:
     * $params = [
     *     'key' => '',
     *     'cipher' => 'aes-128',
     *     'mode' => 'cbc',
     *     'base64' => false,
     *     'hmac' => true,
     *     'hmac_digest' => 'sha512',
     * ];
     */
    public function __construct(array $params = [])
    {
        if (version_compare(PHP_VERSION, '7.1.2') < 0) {
            log_message('error', 'Version 7.1.2 or greater is required.');
        }

        if (!extension_loaded('openssl')) {
            log_message('error', 'Encryption: Unable to find an available encryption driver.');
        }

        if (!empty($params['key'])) {
            $this->key = $params['key'];
        }
        if (empty($this->key) && self::strlen($key = config_item('encryption_key')) > 0) {
            $this->key = $key;
        }

        if (!empty($params['cipher'])) {
            $params['cipher'] = strtolower($params['cipher']);
            $this->cipher = $params['cipher'];
        }

        if (!empty($params['mode'])) {

            $params['mode'] = strtolower($params['mode']);
            if (isset($this->_modes[$params['mode']])) {
                $this->mode = $this->_modes[$params['mode']];
            } else {
                log_message('error', 'Encryption: OpenSSL mode '.strtoupper($params['mode']).' is not available.');
            }
        }

        $handle = empty($this->mode) ? $this->cipher : $this->cipher.'-'.$this->mode;
        if (in_array($handle, openssl_get_cipher_methods(), true)) {
            $this->handle = $handle;
        } else {
            $this->handle = '';
            log_message('error', 'Encryption: Unable to initialize OpenSSL with method '.strtoupper($handle).'.');
        }

        if (!empty($params['base64'])) {
            $this->base64 = $params['base64'];
        }

        if (!empty($params['hmac'])) {
            $this->hmac = $params['hmac'];
        }

        if (!empty($params['hmac_digest'])) {
            $this->hmac_digest = $params['hmac_digest'];
        }
    }

    /**
     * Create a random key
     *
     * @param int $length Output length
     * @return string
     */
    public function create_key($length)
    {
        if (function_exists('random_bytes')) {
            try {
                return random_bytes((int) $length);
            } catch (Exception $e) {
                log_message('error', $e->getMessage());
                return false;
            }
        }

        $is_secure = null;
        $key = openssl_random_pseudo_bytes($length, $is_secure);
        return ($is_secure === true) ? $key : false;
    }

    /**
     * Encrypt
     *
     * string $data data
     * @return mixed
     */
    public function encrypt($data)
    {
        $key = hash_hkdf('sha512', $this->key, self::strlen($this->key), 'encryption');
        if (($data = $this->_encrypt($data, $key)) === false) {
            return false;
        }

        $this->base64 && $data = base64_encode($data);

        if ($this->hmac) {
            $hmac_key = hash_hkdf('sha512', $this->key, 0, 'authentication');
            return hash_hmac($this->hmac_digest, $data, $hmac_key, !$this->base64).$data;
        }

        return $data;
    }

    /**
     * Decrypt
     *
     * @param string $data Encrypted data
     * @return mixed
     */
    public function decrypt($data)
    {
        if ($this->hmac) {
            $digest_size = $this->base64 ? $this->_digests[$this->hmac_digest] * 2
                : $this->_digests[$this->hmac_digest];
            if (self::strlen($data) <= $digest_size) {
                return false;
            }

            $hmac_input = self::substr($data, 0, $digest_size);
            $data = self::substr($data, $digest_size);

            $hmac_key = hash_hkdf('sha512', $this->key,  0, 'authentication');
            $hmac_check = hash_hmac($this->hmac_digest, $data, $hmac_key, !$this->base64);

            // Time-attack-safe comparison
            $diff = 0;
            for ($i = 0; $i < $digest_size; $i++)
            {
                $diff |= ord($hmac_input[$i]) ^ ord($hmac_check[$i]);
            }

            if ($diff !== 0) {
                return false;
            }
        }

        if ($this->base64) {
            $data = base64_decode($data);
        }

        $key = hash_hkdf('sha512', $this->key, self::strlen($this->key), 'encryption');

        return $this->_decrypt($data, $key);
    }

    /**
     * Encrypt
     *
     * @param string $data data
     * @param string $key $key
     * @return mixed
     */
    protected function _encrypt($data, $key)
    {
        if (empty($this->handle)) {
            return false;
        }

        $iv = ($iv_size = openssl_cipher_iv_length($this->handle))
            ? $this->create_key($iv_size)
            : null;

        $data = openssl_encrypt(
            $data,
            $this->handle,
            $key,
            1, // DO NOT TOUCH!
            $iv
        );

        if ($data === false) {
            return false;
        }

        return $iv.$data;
    }

    /**
     * Decrypt
     *
     * @param string $data Encrypted data
     * @param string $key $key
     * @return mixed
     */
    protected function _decrypt($data, $key)
    {
        if (empty($this->handle)) {
            return false;
        }

        if ($iv_size = openssl_cipher_iv_length($this->handle)) {
            $iv = self::substr($data, 0, $iv_size);
            $data = self::substr($data, $iv_size);
        } else {
            $iv = null;
        }

        return openssl_decrypt(
            $data,
            $this->handle,
            $key,
            1, // DO NOT TOUCH!
            $iv
        );
    }

    /**
     * Byte-safe strlen()
     *
     * @param string $str
     * @return int
     */
    protected static function strlen($str)
    {
        return mb_strlen($str, '8bit');
    }

    /**
     * Byte-safe substr()
     *
     * @param string $str
     * @param int $start
     * @param int $length
     * @return string
     */
    protected static function substr($str, $start, $length = null)
    {
        // mb_substr($str, $start, null, '8bit') returns an empty
        // string on PHP 5.3
        isset($length) OR $length = ($start >= 0 ? self::strlen($str) - $start : -$start);
        return mb_substr($str, $start, $length, '8bit');
    }
}