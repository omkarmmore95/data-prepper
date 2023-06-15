/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class AvroOutputCodecConfig {

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("schema_file_location")
    private String fileLocation;

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys;

    @JsonProperty("schema_registry_url")
    private String schemaRegistryUrl;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }


    public String getFileLocation() {
        return fileLocation;
    }

    public String getSchema() {
        return schema;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }
}
