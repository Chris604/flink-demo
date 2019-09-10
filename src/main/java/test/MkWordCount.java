package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import stream.WordCount;

public class MkWordCount {
    public static void main(String []args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String>  dStream= env.readTextFile("inout");

        DataSet<Tuple2<String, Integer>> text = dStream.flatMap(new TokenZer()).groupBy(0).sum(1);
//        DataSet<Tuple2<String, Integer>> text = dStream.flatMap(new TokenZer()).sum(1);

//        r.writeAsText("out");
        text.print();
//        env.execute("test2");

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
