<?php
/**
 * Created by PhpStorm.
 * User: gwf
 * Date: 2017/8/23
 * Time: 17:28
 */

class Curl
{
    private $_ch;
    private $_url;

    private $_timeout = 5; // 单位：秒

    private $_info = [];

    function __construct()
    {
        $this->_ch = curl_init();

        curl_setopt($this->_ch, CURLOPT_TIMEOUT, $this->_timeout);
        curl_setopt($this->_ch, CURLOPT_RETURNTRANSFER, true);
    }

    public function get($url, Array $params)
    {
        if(empty($params))
        {
            $this->_info = array(
                'code'=> 1,
                'msg' => "参数错误：参数不能为空",
                'response' => '',
                'response_time' => 0,
            );

            return $this->_info;
        }

        $url .= '?'.http_build_query($params);
        $this->_url = $url;
        curl_setopt($this->_ch, CURLOPT_HTTPGET, true);

        $this->_result();

        return $this->_info;
    }

    public function post($url, Array $params)
    {
        if(empty($params))
        {
            $this->_info = array(
                'code'=> 1,
                'msg' => "参数错误：参数不能为空",
                'response' => '',
                'response_time' => 0,
            );

            return $this->_info;
        }

        $this->_url = $url;
        curl_setopt($this->_ch, CURLOPT_POST, true);
        curl_setopt($this->_ch, CURLOPT_POSTFIELDS, $params);

        $this->_result();

        return $this->_info;
    }

    /*
     * 设置超时时间
     */
    public function setTimeout($timeout)
    {
        $this->_timeout = $timeout;
    }

    private function _result()
    {
        curl_setopt($this->_ch, CURLOPT_URL, $this->_url);
        $response = curl_exec($this->_ch);

        $response_time = curl_getinfo($this->_ch, CURLINFO_TOTAL_TIME);

        $curl_errno = curl_errno($this->_ch);
        if($curl_errno)
        {
            curl_close($this->_ch);
            $this->_info = array(
                'code'=> 2,
                'msg' => "CURL操作失败，错误号：".$curl_errno,
                'response' => '',
                'response_time' => $response_time,
            );

            return ;
        }

        $http_status_code = curl_getinfo($this->_ch, CURLINFO_HTTP_CODE);
        if($http_status_code !== 200)
        {
            curl_close($this->_ch);
            $this->_info = array(
                'code'=> 3,
                'msg' => "请求失败，HTTP代码：".$http_status_code,
                'response' => '',
                'response_time' => $response_time,
            );

            return ;
        }

        curl_close($this->_ch);
        $this->_info = array(
            'code' => 0,
            'msg' => "OK",
            'response' => $response,
            'response_time' => $response_time,
        );
    }
}