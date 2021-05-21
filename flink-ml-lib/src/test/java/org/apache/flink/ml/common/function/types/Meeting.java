package org.apache.flink.ml.common.function.types;

import org.apache.flink.table.annotation.DataTypeHint;

import java.sql.Timestamp;

public class Meeting {
    public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)
    Timestamp time;
    public String event;

    public Meeting(){
        time = new Timestamp(1);
        event = "default meeting event";
    }

    public Meeting(int millis, String event){
        this.time = new Timestamp(millis);
        this.event = event;
    }

    @Override
    public int hashCode() {
        return time.hashCode() * 31 + event.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Meeting)){
            return false;
        }
        Meeting meeting = (Meeting) obj;
        return this.time.equals(meeting.time) && this.event.equals(meeting.event);
    }
}
