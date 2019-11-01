package sql;

public class Cource {
    String c_no;
    String s_no;

    public Cource() {
    }

    public Cource(String c_no, String s_no) {
        this.c_no = c_no;
        this.s_no = s_no;
    }

    public String getC_no() {
        return c_no;
    }

    public void setC_no(String c_no) {
        this.c_no = c_no;
    }

    public String getS_no() {
        return s_no;
    }

    public void setS_no(String s_no) {
        this.s_no = s_no;
    }

    @Override
    public String toString() {
        return "Cource{" +
                "c_no='" + c_no + '\'' +
                ", s_no='" + s_no + '\'' +
                '}';
    }
}