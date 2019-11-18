package online_view;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

// 统计 ab 测试各实验 pv 实时数据
public class OnlineView {

    public static void main(String []args) throws Exception {
        // 初始化 env，并进行相关参数配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
        env.setParallelism(1);

        // 配置 kafka，指定 data source
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","");
        prop.setProperty("zookeeper.connect","");
        prop.setProperty("group.id","online-view");

        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>("abtest", new SimpleStringSchema(), prop);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        SingleOutputStreamOperator<Experiments> mapStream = dataSource.map(new MyMap()).name("map_exp").uid("map_exp");

        // 对数据源开一个窗口，同时设定触发器
        WindowedStream<Experiments, Tuple, TimeWindow> winStream = mapStream.keyBy("eName")
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        SingleOutputStreamOperator<UserStat> aggregate = winStream.aggregate(new MyAggr());

//        SingleOutputStreamOperator<String> map = aggregate.map(value -> value.eName + ":" + value.PV);
        SingleOutputStreamOperator<Tuple2<String, Long>> map = aggregate.map(new MapFunction<UserStat, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserStat value) throws Exception {
                return new Tuple2(value.eName, value.PV);
            }
        });

        map.print();

        // sink 到 redis 中去，引入开源redisSink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder()
                .setHost("172.16.11.245")
                .setPort(6379)
                .build();
        RedisSink<Tuple2<String, Long>> redisSink = new RedisSink<>(conf, new RedisMapper<Tuple2<String, Long>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "flink");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Long> value) {
                return value.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Long> value) {
                return value.f1.toString();
            }
        });

//        map.addSink(redisSink);

        env.execute("online-view");
    }
}

// 自定义一个聚合函数
class MyAggr implements AggregateFunction<Experiments, UserStat, UserStat>{

    @Override
    public UserStat createAccumulator() {
        return new UserStat();
    }

    @Override
    public UserStat add(Experiments value, UserStat accumulator) {

        accumulator.seteName(value.eName);
        if (accumulator.getPV() == null) {
            accumulator.PV = 0L;
        }
        accumulator.setPV(accumulator.PV + 1);
        return accumulator;
    }

    @Override
    public UserStat getResult(UserStat accumulator) {
        return accumulator;
    }

    @Override
    public UserStat merge(UserStat a, UserStat b) {
        a.setPV(a.getPV() + b.getPV());
        return a;
    }
}

// 自定义 map 函数
class MyMap implements MapFunction<String, Experiments> {

    private Experiments exp = new Experiments();

    @Override
    public Experiments map(String value) throws Exception {

        JSONObject jsonObject = JSON.parseObject(value);
        exp.setUserid(jsonObject.getString("userid"));
        exp.setdName(jsonObject.getString("domain_name"));
        exp.setlName(jsonObject.getString("layer_name"));
        exp.seteName(jsonObject.getString("experiment_name"));

        return exp;
    }
}

