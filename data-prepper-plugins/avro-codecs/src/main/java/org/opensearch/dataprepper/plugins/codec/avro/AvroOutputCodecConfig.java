/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;


public class AvroOutputCodecConfig {

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("schema_file_location")
    private String fileLocation;


    public String getFileLocation() {
        return fileLocation;
    }

    public String getSchema() {
        return schema;
    }
}
