package sql;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * flink-sql 批量数据
 */
public class FlinkBatchSqlTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WC> input = env.fromElements(
                new WC("nihao", 1),
                new WC("lisi", 1),
                new WC("zhansan", 1),
                new WC("nihao", 3));

        tEnv.registerDataSet("tableTest", input, "word,count");
        Table table = tEnv.sqlQuery("select word, avg( `count`) as `count` from tableTest group by word");

        DataSet<WC> result = tEnv.toDataSet(table, WC.class);

        result.print();
    }

}