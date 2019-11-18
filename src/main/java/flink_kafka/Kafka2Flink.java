package flink_kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import javax.security.auth.login.Configuration;
import java.util.Properties;

public class Kafka2Flink {

    public static void main(String []args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","");
        prop.setProperty("zookeeper.connect","");
        prop.setProperty("group.id","flink-test");

        FlinkKafkaConsumer09 fkc = new FlinkKafkaConsumer09<>("article", new SimpleStringSchema(), prop);
        DataStream text = env.addSource(fkc).name("test01");

//        text.addSink(new FlinkSink());

        text.print();
        env.execute("flink-kafka");

    }



    static class FlinkSink extends RichSinkFunction {
        public static final long serialVersionUID = 1;

        public void open (Configuration configuration) {

        }
        public void close(){

        }

    }
}
