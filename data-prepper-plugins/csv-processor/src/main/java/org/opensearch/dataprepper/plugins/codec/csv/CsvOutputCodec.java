/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as CSV Data
 */
@DataPrepperPlugin(name = "csv", pluginType = OutputCodec.class, pluginConfigurationType = CsvOutputCodecConfig.class)
public class CsvOutputCodec implements OutputCodec {
    private final CsvOutputCodecConfig config;
    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputCodec.class);
    private static final String CSV = "csv";
    private static int headerLength = 0;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @DataPrepperPluginConstructor
    public CsvOutputCodec(final CsvOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
        final List<String> headerList = config.getHeader();
        headerLength = headerList.size();
        final byte[] byteArr = String.join(config.getDelimiter(), headerList).getBytes();
        writeByteArrayToOutputStream(outputStream, byteArr);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        Map<String, Object> eventMap = event.toMap();

        if(!Objects.isNull(config.getExcludeKeys())){
            for (String key : config.getExcludeKeys()) {
                if (eventMap.containsKey(key)) {
                    eventMap.remove(key);
                }
            }
        }

        for (Map.Entry entry : eventMap.entrySet()) {
            Object mapValue = entry.getValue();
            entry.setValue(objectMapper.writeValueAsString(mapValue));
        }

        List<String> valueList = eventMap.entrySet().stream().map(map -> map.getValue().toString())
                .collect(Collectors.toList());
        if (headerLength != valueList.size()) {
            LOG.error("CSV Row doesn't conform with the header.");
            return;
        }
        final byte[] byteArr = valueList.stream().collect(Collectors.joining(",")).getBytes();
        writeByteArrayToOutputStream(outputStream, byteArr);
    }

    private void writeByteArrayToOutputStream(final OutputStream outputStream, final byte[] byteArr) throws IOException {
        outputStream.write(byteArr);
        outputStream.write(System.lineSeparator().getBytes());
    }

    @Override
    public String getExtension() {
        return CSV;
    }
}
