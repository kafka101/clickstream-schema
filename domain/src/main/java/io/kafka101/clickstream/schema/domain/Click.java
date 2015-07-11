package io.kafka101.clickstream.schema.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Click {

    public final String time;
    public final String ip;
    public final String page;

    @JsonCreator
    public Click(@JsonProperty("time") String time,
            @JsonProperty("ip") String ip,
            @JsonProperty("page") String page) {
        this.time = time;
        this.ip = ip;
        this.page = page;
    }
}
