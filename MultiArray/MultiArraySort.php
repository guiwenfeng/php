<?php
/**
 * Created by PhpStorm.
 * User: gwf
 * Date: 2017/8/7
 * Time: 15:03
 */

if ( !function_exists('array_field_sort') )
{
    /*
     * 二维数组按单一字段排序
     */
    function array_field_sort(Array $multiarray, $field, $sort = SORT_ASC)
    {
        if(empty($field)) return false;

        if(!empty($multiarray))
        {
            $keys = [];
            foreach ($multiarray as $row)
            {
                if(is_array($row) && isset($row[$field]))
                {
                    $keys[] = $row[$field];
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            return [];
        }
        array_multisort($keys,$sort,$multiarray);
        return $multiarray;
    }
}

if ( !function_exists('array_multifield_sort') )
{
    /*
     * 二维数组按多个字段排序
     * $rule 排序规则，如：["name"=>SORT_ASC,"date"=>SORT_DESC]
     */
    function array_multifield_sort(Array $multiarray, $rule)
    {
        if(empty($rule)) return false;

        if(!empty($multiarray))
        {
            $args = [];
            foreach($rule as $field => $sort)
            {
                $temp = [];
                foreach($multiarray as $val)
                {
                    $temp[] = $val[$field];
                }
                $args[] = $temp;
                $args[] = $sort;
            }
            $args[] = &$multiarray;
            call_user_func_array('array_multisort',$args);
        }
        else
        {
            return [];
        }

        return $multiarray;
    }
}