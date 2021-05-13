package io.github.xlblog.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SourceWatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每100ms生成一个watermark，大于0，EventTime，等于0，ProcessTime
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> streamSource = env.addSource(new SourceFunction<String>() {
            private List<String> list = new ArrayList<>();
            List<Long> watermarks = new ArrayList<>();

            {
                list.add("a1,1617724800000,2");
                list.add("a1,1617724802000,5");
                list.add("a1,1617724804000,5");
                list.add("a1,1617724806000,6");
                list.add("a1,1617724808000,3");
                list.add("a1,1617724810000,4");
                list.add("a1,1617724812000,8");
                list.add("a1,1617724814000,9");
                list.add("a1,1617724816000,1");
                list.add("a1,1617724818000,5");
                list.add("a1,1617724820000,6");
                list.add("a1,1617724822000,7");
                list.add("a1,1617724824000,2");
                list.add("a1,1617724826000,9");
                list.add("a1,1617724828000,6");

                watermarks.add(0L);
                watermarks.add(0L);
                watermarks.add(0L);
                watermarks.add(0L);
                watermarks.add(1617724802000L);
                watermarks.add(1617724806000L);
                watermarks.add(1617724806000L);
                watermarks.add(1617724806000L);
                watermarks.add(1617724808000L);
                watermarks.add(1617724808000L);
                watermarks.add(1617724810000L);
                watermarks.add(1617724812000L);
                watermarks.add(1617724814000L);
                watermarks.add(1617724816000L);
                watermarks.add(1617724816000L);
                watermarks.add(1617724818000L);
                watermarks.add(1617724820000L);
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                int index = 0;
                for (String value : list) {
                    long l = Long.parseLong(value.split(",")[1]);
                    sourceContext.collectWithTimestamp(value, l);
                    sourceContext.emitWatermark(new Watermark(watermarks.get(index)));
                    System.out.println(value + " watermark:" + watermarks.get(index));
                    index++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sum = streamSource
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple3<>(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, Long, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).timeWindow(Time.seconds(4))
                .sum(2);

        sum.print();
        env.execute();
    }

}
