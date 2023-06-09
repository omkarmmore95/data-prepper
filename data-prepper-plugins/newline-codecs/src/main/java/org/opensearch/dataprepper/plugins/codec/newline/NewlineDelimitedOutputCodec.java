/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.newline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name="newline", pluginType = OutputCodec.class)
public class NewlineDelimitedOutputCodec implements OutputCodec {
    private static final String NDJSON="ndjson";
    private static final String MESSAGE_FIELD_NAME = "message";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @DataPrepperPluginConstructor
    public NewlineDelimitedOutputCodec(){
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final Map<String, Object> eventDataMap = event.toMap();
        if (eventDataMap.keySet().size() > 1) {
            Object headerRecord = null;
            Object messageRecord = null;
            for (final Map.Entry<String, Object> entry : eventDataMap.entrySet()) {
                final String key = entry.getKey();
                final Object value = entry.getValue();
                if (MESSAGE_FIELD_NAME.equals(key)) {
                    messageRecord = value;
                } else {
                    headerRecord = value;
                }
            }
            if(headerRecord!=null)
                writeArrayToOutputStream(outputStream, headerRecord);
            else if(messageRecord!=null)
                writeArrayToOutputStream(outputStream, messageRecord);
        } else {
            writeArrayToOutputStream(outputStream, eventDataMap.get(MESSAGE_FIELD_NAME));
        }
    }

    private void writeArrayToOutputStream(final OutputStream outputStream,final Object object) throws IOException {
        byte[] byteArr = null;
        if(object instanceof Map) {
            //todo
            String json = objectMapper.writeValueAsString(object);
            byteArr = json.toString().getBytes();
        }
        else{
            byteArr = object.toString().getBytes();
        }
        outputStream.write(byteArr);
        outputStream.write(System.lineSeparator().getBytes());
    }

    @Override
    public String getExtension() {
        return NDJSON;
    }
}


