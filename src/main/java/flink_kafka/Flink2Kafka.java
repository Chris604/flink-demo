package flink_kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

import java.util.Properties;

public class Flink2Kafka {
    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","");
        prop.setProperty("zookeeper.connect","");
        prop.setProperty("group.id","flink-test");

        prop.setProperty("buffer.memory", "33554432");
        prop.setProperty("producer.type", "async");
        prop.setProperty("acks", "1");
        prop.setProperty("batch.size", "163840");
        prop.setProperty("linger.ms", "10");
        prop.setProperty("max.request.size", "14805500");

        FlinkKafkaConsumer09 consumer = new FlinkKafkaConsumer09("article", new SimpleStringSchema(), prop);
        FlinkKafkaProducer09 producter = new FlinkKafkaProducer09("exp_test", new SimpleStringSchema(), prop);

        DataStream data = env.addSource(consumer).name("flink-test");

        data.print();

        DataStream mfilter = data.filter(new MyFilter());

        mfilter.addSink(producter);

        env.execute("flink-test");
    }


    static class MyFilter implements FilterFunction {
        @Override
        public boolean filter(Object value) throws Exception {
            return value == "1" ? true:false;
        }
    }
}
