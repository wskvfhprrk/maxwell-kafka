package com.example.k;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author 何建哲
 * @Date 2018/6/14 11:33
 **/
public class Maxwell2MysqlUtil {

    public static String maxwell2Mysql(ConsumerRecord<?, ?> cr) throws Exception {
        //获取key的值
        Map keyMap = getKey(cr);
        //更改为实体类格式
        String value = cr.value().toString();
        Map maxwellMap = JSONObject.toJavaObject((JSON) JSON.parse(value), Map.class);
        //获取sql
        String sql = getSqlMap(keyMap, maxwellMap);
        return sql;
    }

    private static   String getSqlMap(Map keyMap, Map maxwellMap) throws Exception{
        Map dataMap = JSONObject.toJavaObject((JSON) JSON.parse(maxwellMap.get("data").toString()), Map.class);
        String type = maxwellMap.get("type").toString();
        String sql = "";
        if ("insert".equals(type)) {
            sql = insert(maxwellMap, dataMap);
        }
        if ("update".equals(type)) {
            sql = update(maxwellMap, dataMap, keyMap);
        }
        if ("delete".equals(type)) {
            sql = delete1(maxwellMap, keyMap);
        }
        return sql;
    }

    /**
     * 删除sql
     *
     * @param maxwellMap maxweelMap传入的值
     * @param keyMap     key值
     * @return
     */
    private static String delete1(Map maxwellMap, Map keyMap)throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("delete from ");
        sql.append(maxwellMap.get("database"));
        sql.append("." + maxwellMap.get("table"));
        sql.append(" where ");
        sql.append(keyMap.get("key"));
        sql.append(" = ");
        sql.append("'" + keyMap.get("value") + "'");
        return sql.toString();
    }

    /**
     * 修改sql
     *
     * @param maxwellMap maxweelMap传入的值
     * @param dataMap    data值
     * @return
     */
    private static String update(Map maxwellMap, Map dataMap, Map keyMap) throws Exception{
        StringBuffer sql = new StringBuffer();
        sql.append("update ");
        sql.append(maxwellMap.get("database"));
        sql.append("." + maxwellMap.get("table"));
        sql.append(" set ");
        int i = 0;
        int size = dataMap.size();
        for (Object key : dataMap.keySet()) {
            i++;
            //主键不更新除去
            if (!key.toString().equals(keyMap.get("key"))) {
                if (i == size) {
                    sql.append(key + " = '" + dataMap.get(key) + "'");
                } else {
                    sql.append(key + " = '" + dataMap.get(key) + "', ");
                }
            }
        }
        sql.append(" where ");
        sql.append(keyMap.get("key"));
        sql.append(" = ");
        sql.append("'" + keyMap.get("value") + "'");
        return sql.toString();
    }

    /**
     * 添加sql
     *
     * @param maxwellMap maxweelMap传入的值
     * @param dataMap    data值
     * @return
     */
    private static String insert(Map maxwellMap, Map dataMap)throws Exception {
        Map instertMap = instertMap(dataMap);
        StringBuffer sql = new StringBuffer();
        sql.append("insert into ");
        sql.append(maxwellMap.get("database"));
        sql.append(".");
        sql.append(maxwellMap.get("table"));
        sql.append(instertMap.get("tableRowName"));
        sql.append("value");
        sql.append(instertMap.get("tableRow"));
        return sql.toString();
    }

    /**
     * 获取添加中的列头部(aaa,ccc,sss,bbb)和内容('123','1','1','1')
     */
    private static Map instertMap(Map dataMap)throws Exception {
        Map tableMap = new HashMap();
        StringBuffer rowName = new StringBuffer();
        rowName.append(" (");
        StringBuffer row = new StringBuffer();
        row.append(" (");
        int i = 1;
        int size = dataMap.size();
        for (Object key : dataMap.keySet()) {
            //最后一个加逗号
            if (size == i) {
                rowName.append(key + "");
                row.append("'" + dataMap.get(key) + "'");
            } else {
                rowName.append(key + ",");
                row.append("'" + dataMap.get(key) + "',");
            }
            i++;
        }
        rowName.append(") ");
        row.append(")");
        tableMap.put("tableRowName", rowName);
        tableMap.put(("tableRow"), row);
        return tableMap;
    }

    /**
     * 获取key值
     */
    private static Map getKey(ConsumerRecord<?, ?> cr) throws Exception{
        Map keyMap = new HashMap();
        String dataKey = cr.key().toString();
        Map dataKeyMap = JSONObject.toJavaObject((JSON) JSON.parse(dataKey), Map.class);
        for (Object key : dataKeyMap.keySet())
            if (key.toString().indexOf("pk.") > -1) {
                keyMap.put("key", key.toString().replaceAll("pk.", ""));
                keyMap.put("value", dataKeyMap.get(key));
            }
        return keyMap;
    }
}
