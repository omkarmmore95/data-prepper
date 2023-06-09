/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputCodec {

    /**
     * Sink will initiate the start() method of OutputCodec and do initialization of stream
     *
     * @param outputStream
     */
    void start(OutputStream outputStream) throws IOException;

    /**
     * Sink will call complete() method of OutputCodec and do the final wrapping.
     *
     * @param outputStream
     */
    void complete(OutputStream outputStream) throws IOException;

    /**
     * Sink will call writeEvent() - to write schema to output Stream
     *
     * @param event
     * @param outputStream
     */
    void writeEvent(Event event, OutputStream outputStream) throws IOException;

    /**
     * used to get extesion of file
     *
     * @return String
     */
    String getExtension();
}
