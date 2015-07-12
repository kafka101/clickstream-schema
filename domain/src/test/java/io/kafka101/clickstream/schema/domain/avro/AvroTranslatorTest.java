package io.kafka101.clickstream.schema.domain.avro;

import io.kafka101.clickstream.schema.domain.Click;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AvroTranslatorTest {

    @Test
    public void translationTest() throws Exception {
        Schema schema = AvroTranslator.schemaFor(Click.class);
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        String nowAsISO = df.format(new Date());
        Click click = new Click(nowAsISO, "192.168.0.1", "index.html");

        GenericRecord record = AvroTranslator.toRecord(click, schema);
        Click click2 = AvroTranslator.toObject(record);
        assertThat(click.ip, is(equalTo(click2.ip)));
        assertThat(click.time, is(equalTo(click2.time)));
        assertThat(click.page, is(equalTo(click2.page)));
    }
}
