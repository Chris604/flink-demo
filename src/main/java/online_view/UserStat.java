package online_view;

import java.io.Serializable;

public class UserStat implements Serializable {

    public String eName;
    public Long PV;

    public UserStat() {
    }

    public UserStat(String eName, Long PV) {
        this.eName = eName;
        this.PV = PV;
    }

    public String geteName() {
        return eName;
    }

    public void seteName(String eName) {
        this.eName = eName;
    }

    public Long getPV() {
        return PV;
    }

    public void setPV(Long PV) {
        this.PV = PV;
    }

    @Override
    public String toString() {
        return "UserStat{" +
                "eName='" + eName + '\'' +
                ", PV=" + PV +
                '}';
    }
}
