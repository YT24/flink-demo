package org.example.flinkdemo.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yangte
 * @description TODO
 * @date 2024/6/14 09:10
 */
public class MysqlToDorisTest {

    public static void main(String[] args) throws Exception {
        // 测试读取mysql数据
        //testReadMysqlChangeData();


    }

    private static void testReadMysqlChangeData() throws Exception {
        // 获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启cdc checkpoin

        // 使用flinkcdc 构建mysqlSource
        MySqlSource<String> mySqlSource =MySqlSource.<String>builder()
                        .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root1234")
                .databaseList("yangt")
                .tableList("")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //读取数据
        DataStreamSource<String> stringDataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source_1");

        //打印
        stringDataStreamSource.print();

        //启动
        env.execute();
    }

    private static String getTranformSql() {
        String transformSQL =
                " INSERT INTO sink_test_cdc(id,name)\n" +
                        " SELECT " +
                            "id,\n" +
                            "name\n" +
                            "null as __DORIS_DELETE_SIGN__ \n"+
                            "DATE_FORMAT (LOCALTIMESTAMP, 'yyyyMMdd') as pt \n" +
                        " FROM source_test_cdc";
        return transformSQL;
    }

    private static String getSinkSql() {
        return " CREATE TABLE source_test_cdc (\n" +
                        "id STRING," +
                        "name STRING)\n"+
                        "WITH (" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '192.168.44.233:8030',\n" +
                        "  'table.identifier' = 'cdc.test_cdc',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'Senyao@2021',\n" +
                        "  'sink.properties.format' = 'json',\n" +
                        "  'sink.properties.read_json_by_line' = 'true',\n" +
                        "  'sink.enable-delete' = 'false',"+
                        "  'sink.properties.columns' = 'id,name',"+
                        "  'sink.label-prefix' = 'doris_label_"+ System.currentTimeMillis() +"'" +
                        "," +
                        //对于unique模型，关闭两阶段提交也是可以实现幂等写入的，容错性反而高一些。避免出现label_已存在的问题。
                        "  'sink.enable-2pc' = 'false',"+
                        "  'sink.enable.batch-mode' = 'true',"+
                        "  'sink.flush.queue-size' = '10',"+
                        "  'sink.buffer-flush.max-rows' = '50000',"+
                        "  'sink.buffer-flush.max-bytes' = '20Mb',"+
                        "  'sink.buffer-flush.interval' = '30s'"+
                        ")";
    }
}
