package sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

public class JoinSQLExample {

    public static void main(String []args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);

        DataSet<Student> sdata = env.fromElements(new Student("s001", "zhansan"),
                new Student("s002", "zhaosi"),
                new Student("s003", "wangwu"),
                new Student("s004", "liliu"));

        DataSet<Cource> cdata = env.fromElements(new Cource("c001", "s001"),
                new Cource("c001", "s002"),
                new Cource("c002", "s001"),
                new Cource("c003", "s003"),
                new Cource("c003", "s004"));


        Table stable = tenv.fromDataSet(sdata, "s_no,s_name");
        tenv.registerTable("student", stable);

        Table ctable = tenv.fromDataSet(cdata, "c_no, s_no");
        tenv.registerTable("cource", ctable);

        Table table1 = tenv.sqlQuery("select * from student");
        Table table2 = tenv.sqlQuery("select * from cource");

        table1.printSchema();
        table2.printSchema();

        tenv.toDataSet(table1, Student.class).print();
        tenv.toDataSet(table2, Cource.class).print();


        Table table3 = tenv.sqlQuery("select student.s_no, student.s_name, cource.c_no " +
                "from student left join cource " +
                "on student.s_no = cource.s_no");

        table3.writeToSink(new CsvTableSink("./test", ","));

        table3.printSchema();

        env.execute("test");
    }
}
