package org.embulk.parser.fluentd_out_file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parser.fluentd_out_file.FluentdOutFileParserPlugin.PluginTask;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.LocalFileInputPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestFluentdOutFileParserPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private FluentdOutFileParserPlugin plugin;
    private FileInputRunner runner;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        config = config().set("type", "fluentd_out_file");
        plugin = new FluentdOutFileParserPlugin();
        runner = new FileInputRunner(new LocalFileInputPlugin());
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = this.config.deepCopy()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of("name", "date_code", "type", "string"))
                );
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals('\t', task.getDelimiterChar());
    }

    @Test(expected = ConfigException.class)
    public void checkColumnsRequired()
    {
        ConfigSource config = this.config.deepCopy();
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void checkSchemaValidation()
    {
        { // columns size must not be greater than 3.
            ConfigSource config = this.config.deepCopy()
                    .set("columns", ImmutableList.of(
                            ImmutableMap.of("name", "_c0", "type", "string"),
                            ImmutableMap.of("name", "_c1", "type", "string"),
                            ImmutableMap.of("name", "_c2", "type", "string"),
                            ImmutableMap.of("name", "_c3", "type", "string"))
                    );
            try {
                plugin.transaction(config, null);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SchemaConfigException);
            }
        }
        { // columns must not include 'long' and 'double' typed columns.
            ConfigSource config = this.config.deepCopy()
                    .set("columns", ImmutableList.of(
                            ImmutableMap.of("name", "_c0", "type", "long"),
                            ImmutableMap.of("name", "_c1", "type", "double"))
                    );
            try {
                plugin.transaction(config, null);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SchemaConfigException);
            }
        }
    }

    @Test
    public void checkTransaction()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("time", Types.TIMESTAMP, config().set("format", "%Y-%m-%dT%H:%M:%S")),
                column("tag", Types.STRING),
                column("record", Types.JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "2014-06-08T23:59:40\tfile.server.logs\t{\"field1\":\"value1\",\"field2\":\"value2\"}"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(Timestamp.ofEpochSecond(1402271980L), record[0]); // 2014-06-08T23:59:40UTC
            assertEquals("file.server.logs", record[1]);
            assertEquals(newMap(newString("field1"), newString("value1"), newString("field2"), newString("value2")), record[2]);
        }
    }

    private FileInput fileInput(String... lines)
            throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes());
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
            throws IOException
    {
        return new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(inputStreams));
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource config)
    {
        return new ColumnConfig(name, type, config);
    }

    private void transaction(ConfigSource config, final FileInput input)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }
}
