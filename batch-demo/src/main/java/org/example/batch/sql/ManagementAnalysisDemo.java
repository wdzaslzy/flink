package org.example.batch.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class ManagementAnalysisDemo {

    public static void main(String[] args) {
        ExecutionEnvironment environment = new LocalEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(environment);

        /*
         数据结构
            时间戳：6#耗煤速率：7#1-耗煤速率：7#2-耗煤速率：7#3-耗煤速率

         结果：
            某小时各锅炉耗煤量
         */
        Row row1 = Row.of(1603040400000L, 0, 1, 2, 3);
        Row row2 = Row.of(1603040400000L, 1, 2, 3, 4);
        Row row3 = Row.of(1603040400000L, 2, 3, 4, 5);

        DataSource<Row> dataSource = environment.fromElements(row1, row2, row3);
        Table table = batchTableEnvironment.fromDataSet(dataSource, $("event_time"), $("boiler_coal_6"), $("boiler_coal_7_1"), $("boiler_coal_7_2"), $("boiler_coal_7_3"));
        batchTableEnvironment.createTemporaryView("t_table", table);

        String sql = "select a.event_time,\n" +
                "       a.boiler_coal_6_hour,\n" +
                "       (a.boiler_coal_7_1_hour + a.boiler_coal_7_2_hour + a.boiler_coal_7_3_hour) as boiler_coal_7_hour\n" +
                "     from (\n" +
                "         select event_time,\n" +
                "                avg(boiler_coal_6)   as boiler_coal_6_hour,\n" +
                "                avg(boiler_coal_7_1) as boiler_coal_7_1_hour,\n" +
                "                avg(boiler_coal_7_2) as boiler_coal_7_2_hour,\n" +
                "                avg(boiler_coal_7_3) as boiler_coal_7_3_hour\n" +
                "         from t_table\n" +
                "         group by event_time) a";

        Table resultTable = batchTableEnvironment.sqlQuery(sql);

        DataSet<Row> rowDataSet = batchTableEnvironment.toDataSet(resultTable, Row.class);

        rowDataSet.output(new PrintingOutputFormat<>());

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
