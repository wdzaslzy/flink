package io.github.xlblog.watermark;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class JoinDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        DataStreamSource<String> sourceStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int index = 0;
                while (true) {
                    ctx.collectWithTimestamp("hello_" + index, System.currentTimeMillis());
                    /*if (index % 1000000 == 0) {
                        ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                        System.out.println("激活窗口");
                    }*/
                    ctx.emitWatermark(new Watermark(123L));
                    index++;
                    System.out.println(index);
                }
            }

            @Override
            public void cancel() {

            }
        });

        sourceStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] s = value.split("_");
                return new Tuple2<>(s[0], Integer.parseInt(s[1]));
            }
        }).keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>>() {
                    @Override
                    public List<Tuple2<String, Integer>> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<Tuple2<String, Integer>> add(Tuple2<String, Integer> value, List<Tuple2<String, Integer>> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public List<Tuple2<String, Integer>> getResult(List<Tuple2<String, Integer>> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public List<Tuple2<String, Integer>> merge(List<Tuple2<String, Integer>> a, List<Tuple2<String, Integer>> b) {
                        a.addAll(b);
                        return a;
                    }
                }).print();

        env.execute();
    }

}
