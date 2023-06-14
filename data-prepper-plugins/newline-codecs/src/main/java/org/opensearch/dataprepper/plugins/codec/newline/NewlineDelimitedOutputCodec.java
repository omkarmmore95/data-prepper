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
@DataPrepperPlugin(name = "newline", pluginType = OutputCodec.class, pluginConfigurationType = NewlineDelimitedOutputConfig.class)
public class NewlineDelimitedOutputCodec implements OutputCodec {
    private static final String NDJSON = "ndjson";
    private static final String MESSAGE_FIELD_NAME = "message";
    private final String headerDestination;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    NewlineDelimitedOutputConfig config;

    @DataPrepperPluginConstructor
    public NewlineDelimitedOutputCodec(final NewlineDelimitedOutputConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
        headerDestination = config.getHeaderDestination();
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final Map<String, Object> eventDataMap = event.toMap();
        if (eventDataMap.containsKey(headerDestination) || eventDataMap.containsKey(MESSAGE_FIELD_NAME)) {
            final Object headerValue = eventDataMap.get(headerDestination);
            if (headerValue != null) {
                writeArrayToOutputStream(outputStream, headerValue);
                eventDataMap.remove(headerDestination);
            }

            if (eventDataMap != null) {
                final Object value = eventDataMap.get(MESSAGE_FIELD_NAME);
                if (value != null) {
                    writeArrayToOutputStream(outputStream, value);
                }
            }
        } else {
            writeArrayToOutputStream(outputStream, eventDataMap);
        }
    }

    private void writeArrayToOutputStream(final OutputStream outputStream, final Object object) throws IOException {
        boolean isExcludeKeyAvailable = !Objects.isNull(config.getExcludeKeys());
        byte[] byteArr = null;
        if (object instanceof Map) {
            //todo
            Map map = objectMapper.convertValue(object, Map.class);
            if (isExcludeKeyAvailable) {
                for (String key : config.getExcludeKeys()) {
                    if (map.containsKey(key)) {
                        map.remove(key);
                    }
                }
            }
            String json = objectMapper.writeValueAsString(map);
            byteArr = json.getBytes();
        } else {
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


