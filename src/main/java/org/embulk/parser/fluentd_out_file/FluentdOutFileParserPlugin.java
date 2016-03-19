package org.embulk.parser.fluentd_out_file;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.util.Timestamps;
import org.msgpack.value.Value;

public class FluentdOutFileParserPlugin
        implements ParserPlugin
{
    // @see http://docs.fluentd.org/articles/out_file#format

    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.Task
    {
        @Config("delimiter")
        @ConfigDefault("\"\\t\"")
        char getDelimiterChar();

        @Config("columns")
        SchemaConfig getSchemaConfig();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getSchemaConfig().toSchema();
        validateSchema(schema);

        control.run(task.dump(), schema);
    }

    private void validateSchema(Schema schema)
    {
        if (schema.getColumnCount() > 3) {
            throw new SchemaConfigException("The size of columns must not be greater than 3: " + schema.getColumnCount());
        }

        for (Column column : schema.getColumns()) {
            if (!column.getType().equals(Types.TIMESTAMP) &&
                    !column.getType().equals(Types.STRING) &&
                    !column.getType().equals(Types.JSON)) {
                throw new SchemaConfigException("columns must not include 'long' and 'double' types.");
            }
        }
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());
        final JsonParser jsonParser = new JsonParser();

        long lineNumber;
        int linePos;
        String line;
        int columnIndex;

        try (final PageBuilder pageBuilder = newPageBuilder(schema, output);
                final LineDecoder decoder = new LineDecoder(input, task)) {
            while (decoder.nextFile()) {
                lineNumber = 0;

                while ((line = decoder.poll()) != null) {
                    lineNumber++;
                    linePos = 0;
                    columnIndex = 0;

                    // parse time
                    if (schema.getColumn(columnIndex).getType().equals(Types.TIMESTAMP)) {
                        Column column = schema.getColumn(columnIndex);

                        int i = line.indexOf(task.getDelimiterChar(), linePos);
                        Timestamp timestamp = timestampParsers[column.getIndex()].parse(line.substring(linePos, i)); // TODO error handling
                        pageBuilder.setTimestamp(column, timestamp);

                        linePos = i + 1;
                        columnIndex += 1;
                    }

                    // parse tag
                    if (schema.getColumn(columnIndex).getType().equals(Types.STRING)) {
                        Column column = schema.getColumn(columnIndex);

                        int i = line.indexOf(task.getDelimiterChar(), linePos);
                        pageBuilder.setString(column, line.substring(linePos, i));

                        linePos = i + 1;
                        columnIndex += 1;
                    }

                    // parse record
                    Column column = schema.getColumn(columnIndex);
                    Value value = jsonParser.parse(line.substring(linePos)); // TODO error handling

                    pageBuilder.setJson(column, value);

                    pageBuilder.addRecord();
                }
                // TODO error handling
            }

            pageBuilder.finish();
        }
    }

    private PageBuilder newPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }
}
