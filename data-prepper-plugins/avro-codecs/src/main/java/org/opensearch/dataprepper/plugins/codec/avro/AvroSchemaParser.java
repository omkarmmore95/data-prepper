/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class AvroSchemaParser {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Schema parseSchemaFromJsonFile(final String location) throws IOException {
        Map<?, ?> map = mapper.readValue(Paths.get(location).toFile(), Map.class);
        Map schemaMap = new HashMap();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            schemaMap.put(entry.getKey(), entry.getValue());
        }
        String json = mapper.writeValueAsString(schemaMap);
        Schema schema = new Schema.Parser().parse(json);
        return schema;
    }
}
