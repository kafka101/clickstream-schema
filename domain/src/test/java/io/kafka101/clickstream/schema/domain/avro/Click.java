package io.kafka101.clickstream.schema.domain.avro;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;

public final class Click extends io.kafka101.clickstream.schema.domain.Click {

    @AvroDefault("\"unknown\"")
    public final String user;

    @Nullable
    public final String origin;

    @AvroIgnore
    private boolean processed = false;

    public Click() {
        super();
        user = null;
        origin = null;
    }

    public Click(String time, String ip, String page, String user, String origin) {
        super(time, ip, page);
        this.user = user;
        this.origin = origin;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }
}

