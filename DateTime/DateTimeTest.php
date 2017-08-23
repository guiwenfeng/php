<?php
/**
 * Created by PhpStorm.
 * User: gwf
 * Date: 2017/8/4
 * Time: 15:22
 */

include "./DateTime.php";

$cases = array(
    "2017-08-04 15:31:26",     //普通用例
    "2017-07-31 15:31:26",     //周一
    "2017-08-06 15:31:26",     //周日
    "2017-02-01 15:31:26",     //平年二月（测试本月、下个月）
    "2017-02-28 15:31:26",     //平年二月（测试本月、下个月）
    "2017-03-01 15:31:26",     //平年三月（测试上个月）
    "2017-03-31 15:31:26",     //平年三月（测试上个月）
    "2016-02-01 15:31:26",     //闰年二月（测试本月、下个月）
    "2016-02-29 15:31:26",     //闰年二月（测试本月、下个月）
    "2016-03-01 15:31:26",     //闰年三月（测试上个月）
    "2016-03-31 15:31:26",     //闰年三月（测试上个月）
    "2017-01-31 15:31:26",     //一月（测试上个月）
    "2016-12-31 15:31:26",     //十二月（测试下个月）
);

foreach($cases as $case)
{
    test($case);
}

function test($case)
{
    $datetime = new commonDateTime($case);

    echo "========================= $case ============================<br />";

    echo "当天开始时间 -- 当天结束时间";echo "<br />";
    echo $datetime->starttimeToday();echo " -- ";echo $datetime->endtimeToday();echo "<br />";

    echo "本周开始日期 -- 本周结束日期";echo "<br />";
    echo $datetime->startdateWeek();echo " -- ";echo $datetime->enddateWeek();echo "<br />";

    echo "上个月第一天 -- 上个月最后一天";echo "<br />";
    echo $datetime->firstdayLastMonth();echo " -- ";echo $datetime->lastdayLastMonth();echo "<br />";

    echo "上个月开始时间 -- 上个月结束时间";echo "<br />";
    echo $datetime->starttimeLastMonth();echo " -- ";echo $datetime->endtimeLastMonth();echo "<br />";

    echo "上个月的今天";echo "<br />";
    echo $datetime->todayLastMonth();echo "<br />";

    echo "本月第一天 -- 本月最后一天";echo "<br />";
    echo $datetime->firstdayThisMonth();echo " -- ";echo $datetime->lastdayThisMonth();echo "<br />";

    echo "本月开始时间 -- 本月结束时间";echo "<br />";
    echo $datetime->starttimeThisMonth();echo " -- ";echo $datetime->endtimeThisMonth();echo "<br />";

    echo "下个月第一天 -- 下个月最后一天";echo "<br />";
    echo $datetime->firstdayNextMonth();echo " -- ";echo $datetime->lastdayNextMonth();echo "<br />";

    echo "下个月开始时间 -- 下个月结束时间";echo "<br />";
    echo $datetime->starttimeNextMonth();echo " -- ";echo $datetime->endtimeNextMonth();echo "<br />";

    echo "下个月的今天";echo "<br />";
    echo $datetime->todayNextMonth();echo "<br />";
    echo "=====================================================<br />";
}