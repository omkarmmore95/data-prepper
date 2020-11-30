package com.amazon.situp.model;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.model.source.Source;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PluginTypeTests {


    @Test
    public void testPluginTypeEnumValues() {
        final PluginType sourcePluginType = PluginType.SOURCE;
        checkPluginTypeValues(sourcePluginType, "source", Source.class);

        final PluginType bufferPluginType = PluginType.BUFFER;
        checkPluginTypeValues(bufferPluginType, "buffer", Buffer.class);

        final PluginType processorPluginType = PluginType.PROCESSOR;
        checkPluginTypeValues(processorPluginType, "processor", Processor.class);

        final PluginType sinkPluginType = PluginType.SINK;
        checkPluginTypeValues(sinkPluginType, "sink", Sink.class);
    }

    private void checkPluginTypeValues(final PluginType pluginType, final String name, final Class<?> pluginClass){
        assertThat("Incorrect plugin type", pluginType.pluginName(), is(name));
        assertThat("Incorrect plugin class type", pluginType.pluginClass(), is(pluginClass));
    }
}