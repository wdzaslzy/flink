package org.example.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountDemo {

    public static void main(String[] args) {
        ExecutionEnvironment environment = new LocalEnvironment();
        DataSource<String> dataSource = environment.fromElements("world", "hello", "flink", "hello", "scala");

        dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        }).groupBy(0).sum(1).output(new PrintingOutputFormat<Tuple2<String, Integer>>());

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
