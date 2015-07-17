package io.kafka101.clickstream.schema.domain.avro;

import io.kafka101.clickstream.schema.domain.Click;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

public class SchemaSerializationTest {

    private TimeZone tz = TimeZone.getTimeZone("UTC");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    private AvroTranslator translator;

    @Before
    public void setUp() {
        df.setTimeZone(tz);
        translator = new AvroTranslator();
    }

    @Test
    public void record() throws IOException, IllegalAccessException {
        File file = File.createTempFile("avro-test", "out");
        Schema schema = translator.namespacelessSchemaFor(Click.class);
        Click click = new Click(nowAsISO(), "192.168.0.1", "index.html");

        DataFileWriter<GenericContainer> writer = createFileWriter(file, schema);
        writer.append(translator.toAvro(click));
        writer.close();

        DataFileReader<GenericRecord> reader = createReader(file, schema);
        GenericRecord record = reader.next();
        assertNotNull(record);
        assertFalse(reader.hasNext());
        reader.close();

        assertThat(click.ip, is(equalTo(record.get("ip").toString())));
        assertThat(click.page, is(equalTo(record.get("page").toString())));
        assertThat(click.time, is(equalTo(record.get("time").toString())));

    }

    private String nowAsISO() {
        return df.format(new Date());
    }

    private <T> DataFileWriter<T> createFileWriter(File file, Schema schema) throws IOException {
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        return dataFileWriter;
    }

    private <T> DataFileReader<T> createReader(File file, Schema schema) throws IOException {
        DatumReader<T> datumReader = new GenericDatumReader<>(schema);
        return new DataFileReader(file, datumReader);
    }

    @Test
    public void union() throws IOException {
        Schema schema = new Schema.Parser().parse(SchemaSerializationTest.class.getResourceAsStream("/IpAddress.avsc"));

        InetAddress ipv6 = InetAddress.getByName("3ffe:1900:4545:3:200:f8ff:fe21:67cf");
        GenericData.Fixed fixedIpv6 = new GenericData.Fixed(SchemaBuilder.fixed("IPv6").size(16), ipv6.getAddress());
        byte[] bytesIpv6 = toByte(fixedIpv6, schema);
        GenericData.Fixed value = toType(bytesIpv6, schema);
        InetAddress address = InetAddress.getByAddress(value.bytes());
        assertThat(ipv6, is(equalTo(address)));

        InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
        GenericData.Fixed fixedIpv4 = new GenericData.Fixed(SchemaBuilder.fixed("IPv4").size(4), ipv4.getAddress());
        byte[] bytesIpv4 = toByte(fixedIpv4, schema);
        value = toType(bytesIpv4, schema);
        address = InetAddress.getByAddress(value.bytes());
        assertThat(ipv4, is(equalTo(address)));
    }

    private <T> byte[] toByte(T object, Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        DatumWriter<T> writer;
        if (object instanceof SpecificRecord) {
            writer = new SpecificDatumWriter<>(schema);
        } else {
            writer = new GenericDatumWriter<>(schema);
        }
        writer.write(object, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private <T> T toType(byte[] data, Schema schema) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        DatumReader<T> reader = new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    @Test
    public void minimalisticSchema() throws IOException {
        Schema schema = new Schema.Parser().parse(
                SchemaSerializationTest.class.getResourceAsStream("/SimpleString.avsc"));

        File file = File.createTempFile("avro-test", "out");
        DataFileWriter<String> dataFileWriter = createFileWriter(file, schema);

        dataFileWriter.append("Kafka");
        dataFileWriter.append("Avro");
        dataFileWriter.append("Schema Registry");
        dataFileWriter.close();

        DataFileReader<Utf8> reader = createReader(file, schema);
        List<String> values = new ArrayList<>();
        while (reader.hasNext()) {
            Utf8 data = reader.next();
            values.add(data.toString());
        }
        reader.close();

        assertThat(values, hasSize(3));
        assertThat(values.toArray(), arrayContaining("Kafka", "Avro", "Schema Registry"));
    }

}
