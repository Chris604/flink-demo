package sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSONObject;

import java.util.Properties;

public class Kafka2Sql {

    public static void main(String []args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bj-dcs-005:9092,bj-dcs-006:9092,bj-dcs-007:9092,bj-dcs-008:9092,bj-dcs-017:9092");
        properties.setProperty("zookeeper.connect","bj-dcs-002:2181,bj-dcs-003:2181,bj-dcs-004:2181");
        properties.setProperty("group.id","kafka-sql-1");

        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>("abtest", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Experiment> map = env.addSource(consumer).map(new MyMap());

        // 引入 table API
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Table table = tenv.fromDataStream(map, "userid,domainName,experimentName,layerName");
        tenv.registerTable("person",table);
        Table result = tenv.sqlQuery("select userid,domainName,experimentName,layerName from person");

        Table userid = result.select("userid");
        tenv.toAppendStream(userid, Experiment.class).print();

//        CsvTableSink csvTableSink = new CsvTableSink("/test.txt", ",");
//        tenv.registerTableSink("CsvSinkTable",new Array("userid"), null
//                , csvTableSink)

//        result.printSchema();
        env.execute("kafka-sql");
    }
}


class MyMap implements MapFunction<String, Experiment>{

    @Override
    public Experiment map(String value) {
        JSONObject jsonMsg = JSONObject.parseObject(value);
        Experiment experiment = new Experiment(jsonMsg.get("userid").toString(),
                jsonMsg.get("domain_name").toString(),
                jsonMsg.get("experiment_name").toString(),
                jsonMsg.get("layer_name").toString());
        return experiment;
    }
}

class MyFlatMap implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String jsonMsg = JSONObject.parseObject(value).toString();
        out.collect(jsonMsg);
    }
}