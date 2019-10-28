package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class TestWaterMark {

    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> wm = textStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            long maxOutOfTime = 10000;
            long currntEventTime;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currntEventTime - maxOutOfTime);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] s = element.split(" ");
                currntEventTime = Long.parseLong(s[0]);
                return currntEventTime;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wm.map(item -> item.split(" ")[1])
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        out.collect(new Tuple2<>(value, 1L));
                    }
                }).keyBy(0).sum(1);


        sum.print();

        env.execute("test");

    }

}
