package org.example.batch.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

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
        DataSet<WC> result = batchTableEnvironment.toDataSet(table, WC.class);

        // Sink
        result.output(new PrintingOutputFormat<>());

        environment.execute();
    }

    public static class WC {
        private String word;
        private Long num;

        public WC(String word, Long num) {
            this.word = word;
            this.num = num;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getNum() {
            return num;
        }

        public void setNum(Long num) {
            this.num = num;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", num=" + num +
                    '}';
        }
    }

}
