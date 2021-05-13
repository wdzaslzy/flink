/*
package org.example.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountSqlDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = new LocalEnvironment();

        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(environment);

        // Source
        DataSource<String> dataSource = environment.fromElements("word", "hello", "flink", "hello", "scala");

        Table wordTable = batchTableEnvironment.fromDataSet(dataSource, $("word"));
        batchTableEnvironment.createTemporaryView("t_word_count", wordTable);

        Table table = batchTableEnvironment.sqlQuery("select word, count(*) as num from t_word_count group by word");
        DataSet<Row> dataSet = batchTableEnvironment.toDataSet(table, Row.class);

        MapOperator<String, Row> test = dataSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String key = "abc$_abc$_abc$_12345678$_";
                String[] split = key.split("\\$_", -1);
                List<Object> list = new ArrayList<>();
                list.add("666");
                Collections.addAll(list, split);
                list.add(12);
                list.add(12);
                return Row.of(list.toArray());
            }
        });

        // Sink
        JDBCOutputFormat output = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://10.0.0.151:58114")
                .setUsername("app_bi")
                .setPassword("app_bi")
                .setQuery("insert into t_data_quality_inspection(id, corpId, uid, device_type, event_time, metric, null_count, zero_count) value (?, ?, ?, ?, ?, ?, ?, ?)")
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.INTEGER, Types.INTEGER})
                .finish();

        test.output(output);

        environment.execute();
    }

}
*/
