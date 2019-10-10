<?php
/**
 * 公钥加密
 * User: gwf
 * Date: 2019/10/9
 * Time: 17:52
 */

class Rsa {

    /**
     * Private key(私钥)
     *
     * @var string
     */
    protected $privateKey = '';

    /**
     * Public key(公钥)
     *
     * @var string
     */
    protected $publicKey = '';

    /**
     * Rsa constructor.
     *
     * @param array $params Configuration parameters
     * @return void
     *
     * Example:
     * $params = [
     *     'public_key_file' => './rsa_public_key.pem',
     *     'private_key_file' => './rsa_private_key.pem',
     * ];
     */
    public function __construct(array $params = []) {

        if (!extension_loaded('openssl')) {
            die('The openssl extension is required.');
        }

        if (isset($params['public_key_file']) && !empty($params['public_key_file'])) {
            if (!file_exists($params['public_key_file'])) {
                die('public key file not exist.');
            }

            $publicKey = openssl_pkey_get_public(file_get_contents($params['public_key_file']));
            if ($publicKey) {
                $this->publicKey = $publicKey;
            } else {
                $this->publicKey = '';
                die('public key invalid.');
            }
        }

        if (isset($params['private_key_file']) && !empty($params['private_key_file'])) {
            if (!file_exists($params['private_key_file'])) {
                die('private key file not exist.');
            }

            $privateKey = openssl_pkey_get_private(file_get_contents($params['private_key_file']));
            if ($privateKey) {
                $this->privateKey = $privateKey;
            } else {
                $this->privateKey = '';
                die('private key invalid.');
            }
        }
    }

    /**
     * Encrypt
     *
     * @param string $data data
     * @param string $encode encode
     * @return mixed
     */
    public function encrypt($data, $encode = 'base64') {

        $cipherText = '';
        if (openssl_public_encrypt($data, $cipherText, $this->publicKey)) {
            $cipherText = $this->encode($cipherText, $encode);
        }

        return $cipherText;
    }

    /**
     * Decrypt
     *
     * @param string $data data
     * @param string $decode decode
     * @return string
     */
    public function decrypt($data, $decode = 'base64') {

        $plainText = '';
        $data = $this->decode($data, $decode);
        openssl_private_decrypt($data, $plainText, $this->privateKey);

        return $plainText;
    }

    /**
     * Signature
     *
     * @param string $data data
     * @param string $encode encode
     * @return string
     */
    public function sign($data, $encode = 'base64') {

        $sign = '';
        if (openssl_sign($data, $sign, $this->privateKey)) {
            $sign = $this->encode($sign, $encode);
        }

        return $sign;
    }

    /**
     * Verify
     *
     * @param string $data data
     * @param string $sign signature
     * @param string $decode decode
     * @return bool
     */
    public function verify($data, $sign, $decode = 'base64') {

        $ret = false;
        $sign = $this->decode($sign, $decode);
        if ($sign !== false) {
            if (openssl_verify($data, $sign, $this->publicKey)) {
                $ret = true;
            } else {
                $ret = false;
            }
        }

        return $ret;
    }

    /**
     * Encode
     *
     * @param string $data data
     * @param string $type type
     * @return string
     */
    private function encode($data, $type = 'base64') {

        switch (strtolower($type)) {
            case 'base64':
                $data = base64_encode($data);
                break;
            case 'hex':
                $data = bin2hex($data);
                break;
            default:
        }

        return $data;
    }

    /**
     * Decode
     *
     * @param string $data data
     * @param string $type type
     * @return string
     */
    private function decode($data, $type = 'base64') {

        switch (strtolower($type)) {
            case 'base64':
                $data = base64_decode($data);
                break;
            case 'hex':
                $data = hex2bin($data);
                break;
            default:
        }

        return $data;
    }
}