package sink;

import online_view.Experiments;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMysql extends RichSinkFunction<Experiments> {

    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into experiment(userid, d_name, l_name, e_name) values(?,?,?,?);";

        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }

        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(Experiments value, Context context) throws Exception {
        ps.setString(1, value.userid);
        ps.setString(2, value.dName);
        ps.setString(3, value.lName);
        ps.setString(4, value.eName);

        ps.executeUpdate();
    }


    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
