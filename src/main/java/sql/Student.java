package sql;

public class Student {
    String s_no;
    String s_name;

    public Student() {
    }

    public Student(String s_no, String s_name) {
        this.s_no = s_no;
        this.s_name = s_name;
    }

    public String getS_no() {
        return s_no;
    }

    public void setS_no(String s_no) {
        this.s_no = s_no;
    }

    public String getS_name() {
        return s_name;
    }

    public void setS_name(String s_name) {
        this.s_name = s_name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "s_no='" + s_no + '\'' +
                ", s_name='" + s_name + '\'' +
                '}';
    }
}