package org.example.flinkdemo.mysql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class TestDoris {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE source_test_cdc (\n" +
                "`id` INT,\n" +
                "`name` STRING,\n" +
                "" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '127.0.0.1',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root1234',\n" +
                "    'database-name' = 'yangt',\n" +
                "    'table-name' = 'test_cdc'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink_test_cdc (\n" +
                "`id` INT,\n" +
                "`name` STRING\n" +
                ") \n" +
                "WITH (" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '192.168.44.248:8034',\n" +
                "      'table.identifier' = 'yangt.cdc_test',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'Senyao@2021',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true',\n" +
                "  'sink.enable-delete' = 'true',"+
                "  'sink.label-prefix' = 'doris_label_stu_a'" +
                ")");

        tableEnv.executeSql("INSERT INTO sink_test_cdc ( id, name) SELECT id, name FROM source_test_cdc ");
    }
}
