package online_view;

import java.io.Serializable;

// 注意：必须要实现 Serializable，否则在流中不能使用
public class Experiments implements Serializable {
    public String userid;
    public String dName;
    public String lName;
    public String eName;

    public Experiments() {
    }

    public Experiments(String userid, String dName, String lName, String eName) {
        this.userid = userid;
        this.dName = dName;
        this.lName = lName;
        this.eName = eName;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getdName() {
        return dName;
    }

    public void setdName(String dName) {
        this.dName = dName;
    }

    public String getlName() {
        return lName;
    }

    public void setlName(String lName) {
        this.lName = lName;
    }

    public String geteName() {
        return eName;
    }

    public void seteName(String eName) {
        this.eName = eName;
    }

    @Override
    public String toString() {
        return "Experiments{" +
                "userid='" + userid + '\'' +
                ", dName='" + dName + '\'' +
                ", lName='" + lName + '\'' +
                ", eName='" + eName + '\'' +
                '}';
    }
}
