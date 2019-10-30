package sql;

public class WC{
    public String word;
    public int count;

    public WC(){
    }

    public WC(String word, int count){
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "name==" + word + "; count==" + count;
    }
}
