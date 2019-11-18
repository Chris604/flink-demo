package stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import sql.NewsEvent;

import javax.annotation.Nullable;
import java.time.ZoneId;
import java.util.Properties;

public class WaterMarkWithKafka {

    public static void main(String[] args) throws Exception {
        // 连接 kafka
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "");
        prop.setProperty("zookeeper.connect", "");
        prop.setProperty("group.id", "kafka-sql-1");

        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>("newsearn_decode", new SimpleStringSchema(), prop);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = env.addSource(consumer);
        DataStream<NewsEvent> watermarkData = dataStream.map(new DeMap()).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<NewsEvent>() {

            Long currentEventTime = 0L;
            final Long maxOutOfTime = 10000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentEventTime - maxOutOfTime);
            }

            @Override
            public long extractTimestamp(NewsEvent element, long previousElementTimestamp) {
                currentEventTime = element.timeStamp;
                return currentEventTime;
            }
        });

        // watermarkData.print();

        // 数据统计，采用 keyby 方式
        /*
        SingleOutputStreamOperator<NewsEvent> sum = watermarkData
                .filter(new FilterFunction<NewsEvent>() {
                        @Override
                        public boolean filter(NewsEvent value) throws Exception {
                            if (value.userid == null) {
                                return false;
                            }
                            return true;
                        }
                    })
                .keyBy("event")
                .window(TumblingEventTimeWindows.of(Time.seconds(20))).sum("timeStamp");
                sum.print();
         */

        // 采用 table API 方式统计
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Table table = tenv.fromDataStream(watermarkData, "userid,event,timeStamp");
        tenv.registerTable("event", table);

        //Table result = tenv.sqlQuery("select event, count(userid) from event group by event");

        Table result = tenv.sqlQuery("select * from event");

        Table event = result.where("userid <> 'null'")
                .groupBy("event")
                .select("userid.max as userid, event, userid.count as timeStamp");

        // table sink to csv
        // event.writeToSink(new CsvTableSink("test.csv", ","));

        // 将 table 数据转成流数据，写入kafka、hdfs等
        DataStream<Tuple2<Boolean, NewsEvent>> tuple2DataStream = tenv.toRetractStream(event, NewsEvent.class);

        SingleOutputStreamOperator<String> tableMap = tuple2DataStream.map(new MapFunction<Tuple2<Boolean, NewsEvent>, String>() {
            @Override
            public String map(Tuple2<Boolean, NewsEvent> value) throws Exception {
                StringBuilder event = new StringBuilder(value.f1.userid);
                event.append("," + value.f1.event);
                event.append("," + value.f1.timeStamp);

                return event.toString();
            }
        });

        tableMap.print();

        // sink to kafka
        // tableMap.addSink(new FlinkKafkaProducer09("exp_test", new SimpleStringSchema(), prop));

        // sink to hdfs
        String hdfsdir = "hdfs://idc-001-namenode1:8020/user/hive/warehouse/recommend.db/test_flink";

        // DateTimeBucketAssigner<String> bucketAssigner = new DateTimeBucketAssigner<IN>(new Path(hdfsdir), ZoneId.of("+05:00"));

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.
                forBulkFormat(new Path(hdfsdir), ParquetAvroWriters.forReflectRecord(String.class))
//                .withBucketAssigner(bucketAssigner)
                .build();

        tableMap.addSink(streamingFileSink);

        env.execute("test");
    }
}


class DeMap implements MapFunction<String, NewsEvent> {

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