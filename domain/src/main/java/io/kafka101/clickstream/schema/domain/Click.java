package io.kafka101.clickstream.schema.domain;

public class Click {

    public final String time, ip, page;

    public Click() {
        this.time = this.ip = this.page = null;
    }

    public Click(String time, String ip, String page) {
        this.time = time;
        this.ip = ip;
        this.page = page;
    }
}
