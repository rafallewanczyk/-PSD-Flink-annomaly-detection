package viper;


import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.LocalDate;
import java.util.Objects;

public class Log {
    public String ip;
    public LocalDateTime timeStamp;
    public String description;
    public String requestType;
    public int count;

    public Log(String logText, int count){
        String [] splitted = logText.split("\\s");
        LocalTime timeStampTime = LocalTime.parse(splitted[0].split("\\.")[0]);
        LocalDate timeStampDay = LocalDate.now();
        timeStamp = LocalDateTime.of(timeStampDay, timeStampTime);
        requestType = splitted[1];

        if(Objects.equals(requestType, "IP")){
            String [] splittedIp = splitted[2].split("\\.");
            ip = String.join(".", splittedIp[0], splittedIp[1], splittedIp[2], splittedIp[3]);
        }else{
            ip = "unknown";
        }

        description = logText;
        this.count = count;
    }

    @Override
    public String toString(){
        return timeStamp.toString() + " " + requestType + " " + ip + " : " + count;
    }
}
