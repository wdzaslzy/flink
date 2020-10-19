package org.example.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DataSource;

public class CustomDemo {

    public static void main(String[] args) {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = environment.fromElements("1", "2", "3");
        dataSource.runOperation(new CustomUnaryOperation<String, Integer>() {
            private DataSet<String> input;

            public void setInput(DataSet<String> inputData) {
                this.input = inputData;
            }

            public DataSet<Integer> createResult() {
                return input.map(new MapFunction<String, Integer>() {
                    public Integer map(String value) throws Exception {
                        return 2 * Integer.parseInt(value);
                    }
                }).filter(new FilterFunction<Integer>() {
                    public boolean filter(Integer value) throws Exception {
                        return value > 2;
                    }
                });
            }
        }).output(new PrintingOutputFormat<Integer>());

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
