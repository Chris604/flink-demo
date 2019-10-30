package sql;

public class Experiment {
    public String userid;
    public String domainName;
    public String experimentName;
    public String layerName;

    public Experiment(){
    }

    public Experiment(String userid, String domainName, String experimentName, String layerName){
        this.domainName = domainName;
        this.experimentName = experimentName;
        this.userid = userid;
        this.layerName = layerName;
    }

    @Override
    public String toString() {
        return  "userid == " + userid + ", domainName == "+domainName + ", experimentName == " + experimentName + ", layerName== " +layerName;
    }
}
