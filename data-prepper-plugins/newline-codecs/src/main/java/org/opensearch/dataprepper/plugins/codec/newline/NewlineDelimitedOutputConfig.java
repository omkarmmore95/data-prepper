/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.newline;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedOutputConfig {

    @JsonProperty("header_destination")
    private String headerDestination;

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    /**
     * The key containing the header line of the S3 object.
     * If this option is specified then each Event will contain a header_destination field.
     *
     * @return The name of the header_destination field.
     */
    public String getHeaderDestination() {
        return headerDestination;
    }
}