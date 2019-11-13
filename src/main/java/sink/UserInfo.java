package sink;

public class UserInfo {

    public String userid;
    public String name;
    public int    age;

    public UserInfo() {
    }

    public UserInfo(String userid, String name, int age) {
        this.userid = userid;
        this.name = name;
        this.age = age;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "userid='" + userid + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
