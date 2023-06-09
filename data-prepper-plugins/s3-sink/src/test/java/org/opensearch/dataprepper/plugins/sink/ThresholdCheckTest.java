/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;

import java.io.IOException;

class ThresholdCheckTest {

    private Buffer inMemoryBuffer;

    @BeforeEach
    void setUp() throws IOException {
        inMemoryBuffer = new InMemoryBufferFactory().getBuffer();

        while (inMemoryBuffer.getEventCount() < 100) {
            inMemoryBuffer.writeEvent(generateByteArray());
        }
    }


    private byte[] generateByteArray() {
        byte[] bytes = new byte[10000];
        for (int i = 0; i < 10000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
