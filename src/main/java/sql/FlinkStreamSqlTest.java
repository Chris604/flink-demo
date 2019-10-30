package sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


/**
 * flink-sql 流式数据
 */
public class FlinkStreamSqlTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> textStream = env.socketTextStream("localhost", 9999);

        Table table = tEnv.fromDataStream(textStream, "word,count");
        tEnv.registerTable("word_count", table);

        Table result = tEnv.sqlQuery("select word, avg( `count`) as `count` from word_count group by word");

//        tEnv.toAppendStream(result, WC.class).print();
        result.printSchema();

        env.execute("test");
    }
}
