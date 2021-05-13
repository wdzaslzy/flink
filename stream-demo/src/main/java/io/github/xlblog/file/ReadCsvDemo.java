package io.github.xlblog.file;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class ReadCsvDemo {

    public static void main(String[] args) throws Exception {
        RowCsvInputFormat csvInput = new RowCsvInputFormat(
                new Path("G:\\文档\\BI重构设计\\demo.csv"),                                        // 文件路径
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING},// 字段类型
                "\n",                                             // 行分隔符
                ",");                                            // 字段分隔符
        // 跳过第一行 表头
        csvInput.setSkipFirstLineAsHeader(false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = env.readFile(csvInput, "G:\\文档\\BI重构设计\\demo.csv");
        streamSource.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row value) throws Exception {
                return value.getField(0).toString();
            }
        }).print();

        env.execute();
    }

}
