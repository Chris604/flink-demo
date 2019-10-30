package sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.Properties;


public class TimeSort2Sql {

    public static void main(String[] args) throws Exception {

        // 连接 kafka
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "bj-dcs-005:9092,bj-dcs-006:9092,bj-dcs-007:9092,bj-dcs-008:9092,bj-dcs-017:9092");
        prop.setProperty("zookeeper.connect","bj-dcs-002:2181,bj-dcs-003:2181,bj-dcs-004:2181");
        prop.setProperty("group.id","kafka-sql-1");

        FlinkKafkaConsumer09 consumer = new FlinkKafkaConsumer09("newsearn_decode",
                new SimpleStringSchema(), prop);
        consumer.setStartFromLatest(); // 最新位置消费

        // 创建 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 连接数据源、过滤处理
        DataStream dataStream = env.addSource(consumer);
        SingleOutputStreamOperator map = dataStream.map(new DeMap());

        // flink-sql 处理
        Table table = tenv.fromDataStream(map, "userid, event, timeStamp");
        tenv.registerTable("event", table);

        Table result = tenv.sqlQuery("select * from event").
                where("userid <> 'null'").
                where("timeStamp <> 1").
                where("event='AppClick'");

        // 打印 schema，虚拟表的 fileds
        result.printSchema();

        // 控制台打印
        tenv.toAppendStream(result, NewsEvent.class).print();

        // sink 到 csv 文件
        result.writeToSink(new CsvTableSink("test.csv", ","));

//        map.print();

        env.execute("test");
    }
}


class DeMap implements MapFunction<String, NewsEvent>{

    @Override
    public NewsEvent map(String value) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String content = jsonObject.get("content").toString();
        JSONObject jContent = JSONObject.parseObject(content);

        long timeStamp = 1l;
        if (jContent.get("time") != null) {
            timeStamp = (Long) jContent.get("time");
        }
        String event = jContent.get("event").toString();
        String properties = jContent.get("properties").toString();
        JSONObject prop = JSONObject.parseObject(properties);
        String userId = null;
        if (prop.get("userId") != null) {
            userId = prop.get("userId").toString();
        }

        NewsEvent newsEvent = new NewsEvent(userId, event, timeStamp);
        return newsEvent;
    }
}

