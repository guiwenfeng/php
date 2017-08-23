<?php
/**
 * Created by PhpStorm.
 * User: gwf
 * Date: 2017/8/2
 * Time: 19:09
 */

class commonDateTime
{
    private $base = 0;

    function __construct($datetime)
    {
        // 必须是完整日期格式：Y-m-d H:i:s
        $pattern = "/^\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}$/";
        if(preg_match($pattern,$datetime))
        {
            $this->base = strtotime($datetime);
        }
    }

    /*
     * 当天开始时间
     */
    public function starttimeToday()
    {
        $starttime = date("Y-m-d 00:00:00",$this->base);

        return $starttime;
    }

    /*
     * 当天结束时间
     */
    public function endtimeToday()
    {
        $endtime = date("Y-m-d 23:59:59",$this->base);

        return $endtime;
    }

    /*
     * 本周开始日期
     */
    public function startdateWeek()
    {
        $j = date("w",$this->base);//本周第几天（周日为0）
        $startdate = date("Y-m-d",strtotime("-".($j?$j-1:6)." days",$this->base));

        return $startdate;
    }

    /*
     * 本周结束日期
     */
    public function enddateWeek()
    {
        $startdate = $this->startdateWeek();
        $enddate = date("Y-m-d",strtotime("$startdate +6 days"));

        return $enddate;
    }

    /*
     * 上个月第一天
     */
    public function firstdayLastMonth()
    {
        $firstday = date("Y-m-01",strtotime(date("Y",$this->base)."-".(date("m",$this->base)-1)."-01"));

        return $firstday;
    }

    /*
     * 上个月最后一天
     */
    public function lastdayLastMonth()
    {
        $firstday = date("Y-m-01",strtotime(date("Y",$this->base)."-".(date("m",$this->base)-1)."-01"));
        $lastday = date("Y-m-d",strtotime("$firstday +1 month -1 day"));

        return $lastday;
    }

    /*
     * 上个月开始时间
     */
    public function starttimeLastMonth()
    {
        $starttime = $this->firstdayLastMonth()." 00:00:00";

        return $starttime;
    }

    /*
     * 上个月结束时间
     */
    public function endtimeLastMonth()
    {
        $timestamp = strtotime(date("Y",$this->base)."-".(date("m",$this->base)-1)."-01");
        $days = date("t",$timestamp);
        $endtime = date("Y-m-$days 23:59:59",$timestamp);

        return $endtime;
    }

    /*
     * 上个月的今天
     */
    public function todayLastMonth()
    {
        $firstday = $this->firstdayLastMonth();

        $timestamp = strtotime($firstday);
        $days = date("t",$timestamp);//上个月的天数
        $j = date("j",$this->base);//第几天
        if($j > $days)
        {
            $d = $days -1;
            $date = date("Y-m-d",strtotime("+$d days",$timestamp));
        }
        else
        {
            $d = $j - 1;
            $date = date("Y-m-d",strtotime("+$d days",$timestamp));
        }

        return $date;
    }

    /*
     * 本月第一天
     */
    public function firstdayThisMonth()
    {
        $firstday = date("Y-m-01",$this->base);

        return $firstday;
    }

    /*
     * 本月最后一天
     */
    public function lastdayThisMonth()
    {
        $days = date("t",$this->base);
        $lastday = date("Y-m-$days",$this->base);

        return $lastday;
    }

    /*
     * 本月开始时间
     */
    public function starttimeThisMonth()
    {
        $starttime = date("Y-m-01 00:00:00",$this->base);

        return $starttime;
    }

    /*
     * 本月结束时间
     */
    public function endtimeThisMonth()
    {
        $days = date("t",$this->base);
        $endtime = date("Y-m-$days 23:59:59",$this->base);

        return $endtime;
    }

    /*
     * 下个月第一天
     */
    public function firstdayNextMonth()
    {
        $datetime = getdate($this->base);
        if($datetime["mon"] == 12)
        {
            $year = $datetime["year"] + 1;
            $firstday = $year."-01-01";
        }
        else
        {
            $firstday = date("Y-m-01",strtotime(date("Y",$this->base)."-".(date("m",$this->base)+1)."-01"));
        }

        return $firstday;
    }

    /*
     * 下个月最后一天
     */
    public function lastdayNextMonth()
    {
        $datetime = getdate($this->base);
        if($datetime["mon"] == 12)
        {
            $year = $datetime["year"] + 1;
            $firstday = $year."-01-01";
            $lastday = date("Y-m-d",strtotime("$firstday +1 month -1 day"));
        }
        else
        {
            $firstday = date("Y-m-01",strtotime(date("Y",$this->base)."-".(date("m",$this->base)+1)."-01"));
            $lastday = date("Y-m-d",strtotime("$firstday +1 month -1 day"));
        }

        return $lastday;
    }

    /*
     * 下个月开始时间
     */
    public function starttimeNextMonth()
    {
        $starttime = $this->firstdayNextMonth()." 00:00:00";

        return $starttime;
    }

    /*
     * 下个月结束时间
     */
    public function endtimeNextMonth()
    {
        $endtime = $this->lastdayNextMonth()." 23:59:59";

        return $endtime;
    }

    /*
     * 下个月的今天
     */
    public function todayNextMonth()
    {
        $firstday = $this->firstdayNextMonth();

        $timestamp = strtotime($firstday);
        $days = date("t",$timestamp);//下个月的天数
        $j = date("j",$this->base);//第几天
        if($j > $days)
        {
            $d = $days -1;
            $date = date("Y-m-d",strtotime("+$d days",$timestamp));
        }
        else
        {
            $d = $j - 1;
            $date = date("Y-m-d",strtotime("+$d days",$timestamp));
        }

        return $date;
    }
}