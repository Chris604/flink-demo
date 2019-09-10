package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class NcWordCount {
    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> r = lines.flatMap(new TokenZer()).keyBy(0).timeWindow(Time.seconds(10)).sum(1);

//        r.writeAsText("out");
        r.print();
        env.execute("test");

    }

    static class TokenZer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");

            for (String word:words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
