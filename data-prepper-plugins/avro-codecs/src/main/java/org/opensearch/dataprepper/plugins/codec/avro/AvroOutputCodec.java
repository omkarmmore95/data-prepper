/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as AVRO Data
 */
@DataPrepperPlugin(name="avro", pluginType = OutputCodec.class, pluginConfigurationType =  AvroOutputCodecConfig.class)
public class AvroOutputCodec implements OutputCodec {

    private final AvroOutputCodecConfig config;

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }


    private static DataFileWriter<GenericRecord> dataFileWriter;

    private static Schema schema;

    private static final String AVRO="avro";

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(config.getSchema());
        schema=parseSchema(config.getSchema());
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, outputStream);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        dataFileWriter.close();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event,final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final GenericRecord record = new GenericData.Record(schema);
        for(final String key: event.toMap().keySet()){
            record.put(key,event.toMap().get(key));
        }
        dataFileWriter.append(record);
    }

    @Override
    public String getExtension() {
        return AVRO;
    }

    static Schema parseSchema(final String schemaString) {
        Objects.requireNonNull(schemaString);
        // todo : Parse schemaString and generate Schema
        /*return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();*/
        return new Schema.Parser().parse(schemaString);

    }

}


