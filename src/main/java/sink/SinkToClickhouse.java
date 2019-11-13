package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SinkToClickhouse extends RichSinkFunction<Map<String,Object>> {

    private final static Logger logger = Logger.getLogger(SinkToClickhouse.class);

    private ClickHouseDataSource dataSource;
    private Connection connection;
    private PreparedStatement ps;
    private List<String> columnNameAndType;
    private String tableName;
    private String schemaName;
    private String url;
    private String user;
    private String password;
    private  int batchCount;
    private  int batchSize=50000;
    private  Long lastBatchTime=System.currentTimeMillis();
    private int batchInterval=3000;
    //
    public SinkToClickhouse(String schemaName, String tableName,String url,String user,String password,int batchSize,int batchInterval){
        this.schemaName=schemaName;
        this.tableName=tableName;
        this.url = url;
        this.user=user;
        this.password=password;
        this.batchSize =batchSize;
        this.batchInterval=batchInterval;
    }

    @Override
    public void open(Configuration parameters) {

        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setUser(user);
            properties.setPassword(password);
            dataSource = new ClickHouseDataSource (url, properties);
            connection = dataSource.getConnection();
            columnNameAndType = getColumnNameAndType(tableName, schemaName, connection);
            String insertSql = generatePreparedSqlByColumnNameAndType(columnNameAndType,schemaName,tableName);
            ps = connection.prepareStatement(insertSql);
        }catch (Exception e){
            logger.error("初始化clickhouse sink失败",e);
        }

    }



    @Override
    public void invoke(Map<String,Object> map)  {


        if(map==null){
            logger.error("sinkclickhouse 数据map为空");
            return;
        }
        try {
            ps=generatePreparedCloumns(ps,columnNameAndType,map);
            //this inserts your data
            ps.addBatch();
            batchCount++;
            // 上次保存时间  现在时间 - 3s

            if (batchCount>=batchSize ||lastBatchTime<System.currentTimeMillis()-batchInterval) {
                batchCount = 0;
                lastBatchTime = System.currentTimeMillis();
                ps.executeBatch();
            }
        } catch (SQLException e) {
            logger.error("初始化clickhouse sink invoke失败",e);
            try {
                logger.error("invoke-json:" + map.toString());
            }catch (Exception e1){
                logger.error(e1);
            }

        }

    }



    private static PreparedStatement  generatePreparedCloumns(PreparedStatement ps, List<String> columnNamesAndType, Map<String,Object> map){

        try{

            for(int i=0;i<columnNamesAndType.size();i++){
                String columnName=columnNamesAndType.get(i).split(":")[0];
                Object value=map.get(columnName);
                String clickhouseType=columnNamesAndType.get(i).split(":")[1];

//                value=String.valueOf(value);
                String convert_value=String.valueOf(value);
                if(convert_value.equals("null") || convert_value.equals("")){
                    convert_value="null";
                }
//                if(value.equals("null") ||value.equals("")){
//                    value=0;
//                }
                if (isNullable(clickhouseType)) {
                    clickhouseType = unwrapNullable(clickhouseType);
                }
                if (clickhouseType.startsWith("Int") || clickhouseType.startsWith("UInt")) {
                    if(convert_value.equals("null")){
                        convert_value="0";
                    }
                    if( clickhouseType.endsWith("64")){
                        ps.setLong(i+1,Long.valueOf(convert_value));

                    }else {
                        ps.setInt(i+1,Integer.valueOf(convert_value));

                    }
                }else  if ("String".equals(clickhouseType)) {
                    if(convert_value.equals("null")){
                        convert_value="null";
                    }
                    ps.setString(i+1,String.valueOf(convert_value));

                }else  if (clickhouseType.startsWith("Float32")) {
                    if(convert_value.equals("null")){
                        convert_value="0";
                    }
                    ps.setFloat(i+1,Float.valueOf(convert_value));

                }else if (clickhouseType.startsWith("Float64")) {
                    if(convert_value.equals("null")){
                        convert_value="0";
                    }
                    ps.setDouble(i+1,Double.valueOf(convert_value));

                }else if ("Date".equals(clickhouseType))
                {
                    if(convert_value.equals("null")){
                        convert_value="0";
                    }
                    ps.setString(i+1,String.valueOf(convert_value));

                }else if ("DateTime".equals(clickhouseType)) {
                    if(convert_value.equals("null")){
                        convert_value="0";
                    }
                    ps.setString(i+1,String.valueOf(convert_value));

                }else if ("FixedString".equals(clickhouseType)) {
                    // BLOB 暂不处理
                    ps.setString(i+1,"ERROR");
                }else if (isArray(clickhouseType)) {
                    //ARRAY 暂不处理
                    ps.setString(i+1,"ERROR");
                }else {
                    ps.setString(i+1,"ERROR");
                }

            }
        }catch (Exception e){
            logger.error("generate:"+map.toString());
            logger.error("初始化clickhouse sinks生成prepared columns异常",e);

        }
        return ps;

    }

    private  static  String  generatePreparedSqlByColumnNameAndType(List<String> columnNames,String schemaName,String tableName){
        String insertColumns = "";
        String insertValues = "";

        if(columnNames != null && columnNames.size() > 0){
            insertColumns += columnNames.get(0).split(":")[0];
            insertValues += "?";
        }


        for(int i = 1; i < columnNames.size();i++){
            insertColumns += ", " + columnNames.get(i).split(":")[0] ;
            insertValues += ", " +"?";
        }

        String insertSql = "INSERT INTO "+schemaName+"."+tableName+" (" + insertColumns + ") values(" + insertValues + ")";
        return insertSql;
    }

    private static List<String> getColumnNameAndType(String tableName, String schemaName,Connection conn) throws  SQLException{

        DatabaseMetaData dd=conn.getMetaData();
        List<String> columnNameAndType =new ArrayList<String>();
        ResultSet colRet = dd.getColumns(null,"%", tableName,"%");
        while(colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            columnNameAndType.add(columnName+":"+columnType);
        }
        return columnNameAndType;
    }

    private static int toSqlType(String clickhouseType) {
        if (isNullable(clickhouseType)) {
            clickhouseType = unwrapNullable(clickhouseType);
        }
        if (clickhouseType.startsWith("Int") || clickhouseType.startsWith("UInt")) {
            return clickhouseType.endsWith("64") ? Types.BIGINT : Types.INTEGER;
        }
        if ("String".equals(clickhouseType)) return Types.VARCHAR;
        if (clickhouseType.startsWith("Float32")) return Types.FLOAT;
        if (clickhouseType.startsWith("Float64")) return Types.DOUBLE;
        if ("Date".equals(clickhouseType)) return Types.DATE;
        if ("DateTime".equals(clickhouseType)) return Types.TIMESTAMP;
        if ("FixedString".equals(clickhouseType)) return Types.BLOB;
        if (isArray(clickhouseType)) return Types.ARRAY;

        // don't know what to return actually
        return Types.VARCHAR;
    }
    private static String unwrapNullable(String clickhouseType) {
        return clickhouseType.substring("Nullable(".length(), clickhouseType.length() - 1);
    }

    private static boolean isNullable(String clickhouseType) {
        return clickhouseType.startsWith("Nullable(") && clickhouseType.endsWith(")");
    }
    private static boolean isArray(String clickhouseType) {
        return clickhouseType.startsWith("Array(")
                && clickhouseType.endsWith(")");
    }



    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if(connection != null){
            connection.close();
        }
    }

}
