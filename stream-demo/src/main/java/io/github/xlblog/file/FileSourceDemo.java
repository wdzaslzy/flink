package io.github.xlblog.file;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;

import java.io.IOException;

public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 直接读文本文件
//        DataStreamSource<String> source = env.readTextFile("F:\\test\\shiji.txt");

        // 2. 使用textInputFormat进行转换
        TextInputFormat format = new TextInputFormat(new Path("hdfs:///flume/4b86061ef403000"));
        format.setNestedFileEnumeration(true);
        format.supportsMultiPaths();
        format.setFilesFilter(new FilePathFilter() {
            @Override
            public boolean filterPath(Path filePath) {
                String path = filePath.getPath();
                System.out.println("current path:" + path);
                try {
                    FileStatus fileStatus = filePath.getFileSystem().getFileStatus(filePath);
                    if (fileStatus.isDir()) {
                        String[] split = path.split("/");
                        String endPath = split[split.length - 1];
                        System.out.println(endPath);
                        if (!endPath.equals("2021-01-01")) {
                            return true;
                        }
                        return false;
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                }
                return false;
            }
        });
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");
        ContinuousFileReaderOperatorFactory<String, TimestampedFileInputSplit> factory =
                new ContinuousFileReaderOperatorFactory<>(format);
        ContinuousFileMonitoringFunction<String> monitoringFunction =
                new ContinuousFileMonitoringFunction<>(format, FileProcessingMode.PROCESS_ONCE, 1, -1);
        SingleOutputStreamOperator<String> source = env.addSource(monitoringFunction).transform("Split Reader: ", typeInfo, factory);

        // 3. 使用JSON转换
//        JsonInputFormat format = new JsonInputFormat("G:\\文档\\BI重构设计\\user.txt");
//        format.setFilesFilter(FilePathFilter.createDefaultFilter());
//        TypeInformation<TestInterface> typeInfo = PojoTypeInfo.of(TestInterface.class);
//        ContinuousFileReaderOperatorFactory<TestInterface, TimestampedFileInputSplit> factory =
//                new ContinuousFileReaderOperatorFactory<>(format);
//        ContinuousFileMonitoringFunction<TestInterface> monitoringFunction =
//                new ContinuousFileMonitoringFunction<>(format, FileProcessingMode.PROCESS_ONCE, 1, -1);
//        SingleOutputStreamOperator<TestInterface> source = env.addSource(monitoringFunction).transform("Split Reader: ", typeInfo, factory);
//
//        source.map(new MapFunction<TestInterface, String>() {
//            @Override
//            public String map(TestInterface value) throws Exception {
//                return value.getField();
//            }
//        }).print();

        source.print();
        env.execute();
    }

}
