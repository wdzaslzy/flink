package org.example.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.types.Row;

import java.sql.Types;

public class WriteIntoDb {

    public static void main(String[] args) {
        ExecutionEnvironment environment = new LocalEnvironment();

        MapOperator<String, Row> row = environment.fromElements("1", "2").map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                Row row = new Row(8);
                row.setField(0, "1235456");
                row.setField(1, "1235456");
                row.setField(2, "1235456");
                row.setField(3, "1235456");
                row.setField(4, 123456789L);
                row.setField(5, 567);
                row.setField(6, 122);
                row.setField(7, 12);
                return row;
            }
        });

        JDBCOutputFormat output = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://10.0.0.151:58114")
                .setUsername("app_bi")
                .setPassword("app_bi")
                .setQuery("insert into t_data_quality_inspection(id, corpId, uid, device_type, event_time, metric, null_count, zero_count) value (?, ?, ?, ?, ?, ?, ?, ?)")
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.INTEGER, Types.INTEGER})
                .finish();
        row.output(output);

    }

}
