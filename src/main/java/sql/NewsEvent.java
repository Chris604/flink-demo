package sql;

public class NewsEvent {
    public String userid;
    public String event;
    public long timeStamp;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public NewsEvent() {
    }

    public NewsEvent(String userid, String event, long timeStamp) {
        this.userid = userid;
        this.event = event;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "NewsEvent{" +
                "userid='" + userid + '\'' +
                ", event='" + event + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
