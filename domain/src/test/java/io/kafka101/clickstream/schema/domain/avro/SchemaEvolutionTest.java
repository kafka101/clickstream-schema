package io.kafka101.clickstream.schema.domain.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class SchemaEvolutionTest {

    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    private AvroTranslator translator;

    @Before
    public void setUp() {
        df.setTimeZone(tz);
        translator = new AvroTranslator();
    }

    private io.kafka101.clickstream.schema.domain.Click generateLegacyEntity() {
        return new io.kafka101.clickstream.schema.domain.Click(df.format(new Date()), "192.168.0.1", "index.html");
    }

    private Click generateCurrentEntity() {
        Click click = new Click(df.format(new Date()), "192.168.0.1", "index.html", "user", "nullableOrigin");
        click.setProcessed(true);
        return click;
    }

    @Test
    public void backward() throws IOException {
        io.kafka101.clickstream.schema.domain.Click legacyClick = generateLegacyEntity();

        // write legacy Click object
        File file = writeDatum(legacyClick);
        // create reader for up-to-date Click class
        DataFileReader<GenericRecord> fileReader = createReader(file, Click.class);

        Click click = null;
        int counter = 0;
        while (fileReader.hasNext()) {
            counter++;
            GenericRecord record = fileReader.next();
            click = translator.toObject(record, Click.class);
        }
        fileReader.close();

        assertThat(counter, is(equalTo(1)));
        assertThat(click.ip, is(equalTo(legacyClick.ip)));
        assertThat(click.time, is(equalTo(legacyClick.time)));
        assertThat(click.page, is(equalTo(legacyClick.page)));
        assertThat(click.user, is(equalTo("unknown")));
        assertNull(click.origin);
        assertFalse(click.isProcessed());
    }

    @Test
    public void forward() throws IOException {
        Click click = generateCurrentEntity();
        // write up-to-date object
        File file = writeDatum(click);
        // create reader for legacy Click class
        DataFileReader<GenericRecord> fileReader = createReader(file,
                io.kafka101.clickstream.schema.domain.Click.class);

        io.kafka101.clickstream.schema.domain.Click legacyClick = null;
        int counter = 0;
        while (fileReader.hasNext()) {
            counter++;
            GenericRecord record = fileReader.next();
            legacyClick = translator.toObject(record, io.kafka101.clickstream.schema.domain.Click.class);
        }
        fileReader.close();

        assertThat(counter, is(equalTo(1)));
        assertThat(click.ip, is(equalTo(legacyClick.ip)));
        assertThat(click.time, is(equalTo(legacyClick.time)));
        assertThat(click.page, is(equalTo(legacyClick.page)));
        assertThat(click.user, is(equalTo("user")));
        assertThat(click.origin, is(equalTo("nullableOrigin")));
        assertThat(click.isProcessed(), is(equalTo(true)));
    }

    private File writeDatum(Object value) throws IOException {
        File file = File.createTempFile("avro-test", "out");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(translator.namespacelessSchemaFor(value.getClass()), file);
        dataFileWriter.append(translator.toAvro(value));
        dataFileWriter.close();
        return file;
    }

    private DataFileReader<GenericRecord> createReader(File file, Class<?> clazz) throws IOException {
        Schema schema = translator.namespacedSchemaFor(clazz);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        return new DataFileReader(file, datumReader);
    }

}
