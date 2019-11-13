package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class SinkTest {

    public static void main(String []args) throws Exception {
        ParameterTool para = ParameterTool.fromArgs(args);

        // 获取 kudu 相关信息
        String kuduMaster = para.get("kuduMaster", "");
        String tableInfo = para.get("tableInfo", "");

        // 获取 cklickhouse 信息
        String schemaName = para.get("schemaName", "");
        String url = para.get("url", "");
        String user = para.get("user", "");
        String password = para.get("password", "");
        String batchSize = para.get("batchSize", "");
        String batchInterval = para.get("batchInterval", "");


        // 初始化 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 生成数据源
        DataStreamSource<UserInfo> dataSource = env.fromElements(new UserInfo("001", "Jack", 18),
                new UserInfo("002", "Rose", 20),
                new UserInfo("003", "Cris", 22),
                new UserInfo("004", "Lily", 19),
                new UserInfo("005", "Lucy", 21),
                new UserInfo("006", "Json", 24));

        SingleOutputStreamOperator<Map<String, Object>> mapSource = dataSource.map(new MapFunction<UserInfo, Map<String, Object>>() {

            @Override
            public Map<String, Object> map(UserInfo value) throws Exception {
                Map<String, Object> map = new HashMap<>();
                map.put("userid", value.userid);
                map.put("name", value.name);
                map.put("age", value.age);
                return map;
            }
        });

        dataSource.print();
        // sink 到 kudu
        mapSource.addSink(new SinkToKudu(kuduMaster, tableInfo, SinkToKudu.INSERT));

        // sink 到 clickhouse
        // mapSource.addSink(new SinkToClickhouse(schemaName, tableInfo, url, user, password, Integer.valueOf(batchSize), Integer.valueOf(batchInterval)));

        env.execute("sink-test");
    }
}
