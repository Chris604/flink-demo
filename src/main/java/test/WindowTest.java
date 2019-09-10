package test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import stream.WordCount;

import javax.annotation.Nullable;

public class WindowTest {
    public static void main(String []args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> scText = env.socketTextStream("localhost", 9999);

        // timeWindow 实例
//        DataStream<Tuple2<String, Integer>> total = scText.flatMap(new WordCount.Tokenizer())
//                .keyBy(0)
//                .timeWindow(Time.seconds(1))
//                .sum(1);
//
//        DataStream<Tuple2<String, Integer>> total = scText.flatMap(new WordCount.Tokenizer())
//                .keyBy(0)
//                .countWindow(5, 2)
//                .sum(1);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000);
        env.addSource(new SensorSource()).assignTimestampsAndWatermarks(new MyAssigner());

        DataStream<Tuple2<String, Integer>> total = scText.flatMap(new WordCount.Tokenizer());

        total.print();
        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static class SensorSource implements SourceFunction {

        @Override
        public void run(SourceContext ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }

    static class MyAssigner implements AssignerWithPeriodicWatermarks{

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Override
        public long extractTimestamp(Object element, long previousElementTimestamp) {
            return 0;
        }
    }
}
