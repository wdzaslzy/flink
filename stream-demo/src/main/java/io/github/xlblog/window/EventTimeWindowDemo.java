package io.github.xlblog.window;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class EventTimeWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每100ms生成一个watermark，大于0，EventTime，等于0，ProcessTime
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> streamSource = env.addSource(new SourceFunction<String>() {
            private List<String> list = new ArrayList<>();

            {
                list.add("a1,1617724800000,2");
                list.add("a1,1617724802000,5");
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

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sum = streamSource
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple3<>(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new TimestampAssignerSupplier<Tuple3<String, Long, Integer>>() {
                            @Override
                            public TimestampAssigner<Tuple3<String, Long, Integer>> createTimestampAssigner(Context context) {
                                return new TimestampAssigner<Tuple3<String, Long, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                };
                            }
                        }))
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
