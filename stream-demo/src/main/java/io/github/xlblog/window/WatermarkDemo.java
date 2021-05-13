package io.github.xlblog.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * 每15s统计一个最小温度
 */
public class WatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每100ms生成一个watermark
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);


        DataStreamSource<String> stream = env.addSource(new RichSourceFunction<String>() {
            private List<String> list = new ArrayList<>();

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
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                int index = 0;
                for (String value : list) {
                    sourceContext.collect(value);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> operator = stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            private long currentTimestamp = Long.MIN_VALUE;

            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                final long newTimestamp = extractAscendingTimestamp(element);
                if (newTimestamp >= this.currentTimestamp) {
                    this.currentTimestamp = newTimestamp;
                }

                return Long.parseLong(element.split(",")[1]);
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
            }

            int index = 1;
            List<Long> watermarks = new ArrayList<>();

            {
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

            public long extractAscendingTimestamp(String value) {
                Long watermark = watermarks.get(index);
                System.out.println(value + " watermark:" + watermark);
                index++;
                return watermark;
            }
        }).map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 设置延迟时间为1s
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> minByOperator = operator.keyBy(new KeySelector<Tuple3<String, Long, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Integer> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.seconds(4))
                .sum(2);


        minByOperator.print();

        env.execute();
    }

}
