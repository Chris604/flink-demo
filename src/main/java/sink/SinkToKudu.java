package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.log4j.Logger;

public class SinkToKudu<UserInfo> extends RichSinkFunction<Map<String, Object>> {

    private final static Logger logger = Logger.getLogger(SinkToKudu.class);

    public final static int INSERT = 1;
    public final static int UPSERT = 2;
    //默认为insert操作
    private int insertOrUpsertFlag = 1;

    private KuduClient client;
    private KuduTable table;

    private String kuduMaster;
    private String tableName;
    private Schema schema;
    private KuduSession session;
    private ByteArrayOutputStream out;
    private ObjectOutputStream os;


    public SinkToKudu(String kuduMaster, String tableName, int insertOrUpsertFlag) {
        this.kuduMaster = kuduMaster;
        this.tableName = tableName;
        this.insertOrUpsertFlag = insertOrUpsertFlag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.error("这不是错误，测试日志采集是否生效使用");
        out = new ByteArrayOutputStream();
        os = new ObjectOutputStream(out);
        client = new KuduClient.KuduClientBuilder(kuduMaster).build();
        table = client.openTable(tableName);
        schema = table.getSchema();
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    }


    @Override
    public void invoke(Map<String, Object> map) {
        if (map == null) {
            logger.error("sinkkudu 数据map为空");
            return;
        }
        try {
            int columnCount = schema.getColumnCount();
            if (insertOrUpsertFlag == SinkToKudu.INSERT) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                for (int i = 0; i < columnCount; i++) {
                    Object value = map.get(schema.getColumnByIndex(i).getName());
                    generateColumnData(row, schema.getColumnByIndex(i).getType(), schema.getColumnByIndex(i).getName(), value);
                }

                OperationResponse response = session.apply(insert);
                if (response != null && response.hasRowError()) {
                    logger.error("事件流插入kudu数据失败:" + response.getRowError().toString() + map.toString());
                }

            } else if (insertOrUpsertFlag == SinkToKudu.UPSERT) {
                Upsert upsert = table.newUpsert();
                PartialRow row = upsert.getRow();
                for (int i = 0; i < columnCount; i++) {
                    Object value = map.get(schema.getColumnByIndex(i).getName());
                    generateUpdateColumnData(row, schema.getColumnByIndex(i).getType(), schema.getColumnByIndex(i).getName(), value);
                }
                OperationResponse response = session.apply(upsert);
                if (response != null && response.hasRowError()) {
                    logger.error("事件流更新kudu数据失败:" + response.getRowError().toString() + map.toString());
                }
            }
        } catch (Exception e) {
            logger.error("kudu 插入或更新数据是invoke异常，可能kudu session关闭,重新打开", e);
            try {
                table = client.openTable(tableName);
            } catch (KuduException e1) {
                logger.error("重新打开kudu表异常", e);
            }
            schema = table.getSchema();
            session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

        }
    }

    @Override
    public void close() throws Exception {
        try {

            session.close();
            client.close();
            os.close();
            out.close();
        } catch (Exception e) {
            logger.error("关闭kudu相关连接，释放资源时异常:" + os + ":" + out + session + ":" + client, e);
            session = null;
            client = null;
            table = null;
            schema = null;
            os = null;
            out = null;
            logger.error("释放资源时异常,将相关连接置为null");
        }
    }


    private void generateColumnData(PartialRow row, Type type, String columnName, Object value) throws IOException {

        try {
            if (value == null) {
                row.setNull(columnName);
                return;
            }

            switch (type) {
                case STRING:
                    row.addString(columnName, String.valueOf(value));
                    return;
                case INT32:
                    row.addInt(columnName, Integer.valueOf(String.valueOf(value)));
                    return;
                case INT64:
                    row.addLong(columnName, Long.valueOf(String.valueOf(value)));
                    return;
                case DOUBLE:
                    row.addDouble(columnName, Double.valueOf(String.valueOf(value)));
                    return;
                case BOOL:
                    row.addBoolean(columnName, (Boolean) value);
                    return;
                case INT8:
                    row.addByte(columnName, (byte) value);
                    return;
                case INT16:
                    row.addShort(columnName, (short) value);
                    return;
                case BINARY:
                    os.writeObject(value);
                    row.addBinary(columnName, out.toByteArray());
                    return;
                case FLOAT:
                    row.addFloat(columnName, Float.valueOf(String.valueOf(value)));
                    return;
                default:
                    throw new UnsupportedOperationException("Unknown type " + type);
            }
        } catch (Exception e) {
            logger.error("类型转换异常当前ROW为 : " + row.toString() + "; 表中TYPE为 : " + type.getName() + "; 当前TYPE为 : " + value.getClass().getName() + "; 当前columnName为 : " + columnName + "; 当前VALUE为 : " + value, e);
        }

    }

    private void generateUpdateColumnData(PartialRow row, Type type, String columnName, Object value) throws IOException {

        try {
            if (value != null) {
//                logger.error("更新用户表的字段"+columnName+" : "+value);
                switch (type) {
                    case STRING:
                        row.addString(columnName, String.valueOf(value));
                        return;
                    case INT32:
                        row.addInt(columnName, Integer.valueOf(String.valueOf(value)));
                        return;
                    case INT64:
                        row.addLong(columnName, Long.valueOf(String.valueOf(value)));
                        return;
                    case DOUBLE:
                        row.addDouble(columnName, Double.valueOf(String.valueOf(value)));
                        return;
                    case BOOL:
                        row.addBoolean(columnName, (Boolean) value);
                        return;
                    case INT8:
                        row.addByte(columnName, (byte) value);
                        return;
                    case INT16:
                        row.addShort(columnName, (short) value);
                        return;
                    case BINARY:
                        os.writeObject(value);
                        row.addBinary(columnName, out.toByteArray());
                        return;
                    case FLOAT:
                        row.addFloat(columnName, Float.valueOf(String.valueOf(value)));
                        return;
                    default:
                        throw new UnsupportedOperationException("Unknown type " + type);
                }
            }
        } catch (Exception e) {
            logger.error("类型转换异常当前ROW为 : " + row.toString() + "; 表中TYPE为 : " + type.getName() + "; 当前TYPE为 : " + value.getClass().getName() + "; 当前columnName为 : " + columnName + "; 当前VALUE为 : " + value, e);
        }

    }
}
