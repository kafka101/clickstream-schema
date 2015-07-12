package io.kafka101.clickstream.schema.domain;

public class Click {

    public String time, ip, page;

    public Click() {
    }

    public Click(String time, String ip, String page) {
        this.time = time;
        this.ip = ip;
        this.page = page;
    }
}
