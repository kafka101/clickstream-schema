package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafka101.clickstream.schema.domain.Click;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SerialializationTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new SerializationModule());
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    @Before
    public void setUp() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        df.setTimeZone(tz);
    }

    @Test
    public void serializeSimplePojo() throws JsonProcessingException {
        Schema schema = SchemaGenerator.generateAvroSchema(Click.class).getAvroSchema();
        String time = df.format(new Date());

        GenericRecord click = new GenericData.Record(schema);
        click.put("time", time);
        click.put("ip", "192.168.0.1");
        click.put("page", "clickstream.html");

        String expected = String.format("{\"ip\":\"192.168.0.1\",\"page\":\"clickstream.html\",\"time\":\"%s\"}", time);
        assertThat(expected, is(equalTo(mapper.writeValueAsString(click))));
    }

    @Test
    public void serializeNestedPojo() throws JsonProcessingException {
        Schema clickSchema = SchemaGenerator.generateAvroSchema(Click.class).getAvroSchema();
        String time = df.format(new Date());
        GenericRecord click = new GenericData.Record(clickSchema);
        click.put("time", time);
        click.put("ip", "192.168.0.1");
        click.put("page", "clickstream.html");

        Schema pojoSchema = SchemaGenerator.generateAvroSchema(NestedPojo.class).getAvroSchema();
        GenericRecord pojo = new GenericData.Record(pojoSchema);
        pojo.put("counter", 42);
        pojo.put("click", click);

        String expected = String.format("{\"ip\":\"192.168.0.1\",\"page\":\"clickstream.html\",\"time\":\"%s\"}", time);
        assertThat(expected, is(equalTo(mapper.writeValueAsString(click))));

        expected = String.format(
                "{\"click\":{\"ip\":\"192.168.0.1\",\"page\":\"clickstream.html\",\"time\":\"%s\"},\"counter\":42}",
                time);
        assertThat(expected, is(equalTo(mapper.writeValueAsString(pojo))));
    }

    public static final class NestedPojo {

        public final Click click;
        public final int counter;

        @JsonCreator
        public NestedPojo(@JsonProperty("click") Click click, @JsonProperty("counter") int counter) {
            this.click = click;
            this.counter = counter;
        }

    }
}
