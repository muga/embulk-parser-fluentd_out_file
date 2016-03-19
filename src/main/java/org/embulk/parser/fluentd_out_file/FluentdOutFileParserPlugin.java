package org.embulk.parser.fluentd_out_file;

import com.google.common.base.Optional;
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
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.LineDecoder;
import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;

public class FluentdOutFileParserPlugin
        implements ParserPlugin
{
    // @see http://docs.fluentd.org/articles/out_file#format

    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.Task, TimestampParser.TimestampColumnOption
    {
        @Config("delimiter")
        @ConfigDefault("\"\\t\"")
        char getDelimiterChar(); // TODO guess?

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%dT%H:%M:%S%Z\"") // iso8601
        String getDefaultTimestampFormat(); // TODO guess?

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        DateTimeZone getDefaultTimeZone();

        @Config("output_time")
        @ConfigDefault("true")
        boolean getOutputTime(); // TODO guess?

        @Config("output_tag")
        @ConfigDefault("true")
        boolean getOutputTag(); // TODO guess?
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump(), newSchema(task));
    }

    private Schema newSchema(PluginTask task)
    {
        Schema.Builder schema = Schema.builder();
        if (task.getOutputTime()) {
            schema.add("time", Types.TIMESTAMP);
        }
        if (task.getOutputTag()) {
            schema.add("tag", Types.STRING);
        }
        schema.add("record", Types.JSON);
        return schema.build();
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final Optional<Integer> timeIndex = task.getOutputTime() ? Optional.of(0) : Optional.<Integer>absent();
        final Optional<Integer> tagIndex = task.getOutputTag() ? (timeIndex.isPresent() ? Optional.of(1) : Optional.of(0)) : Optional.<Integer>absent();
        final int recordIndex = tagIndex.isPresent() ? tagIndex.get() + 1 : 0;
        final Optional<TimestampParser> timestampParser = timeIndex.isPresent() ? Optional.of(new TimestampParser(task, task)) : Optional.<TimestampParser>absent();
        final JsonParser jsonParser = new JsonParser();

        long lineNumber;
        int linePos;
        String line;

        try (final PageBuilder pageBuilder = newPageBuilder(schema, output);
                final LineDecoder decoder = new LineDecoder(input, task)) {
            while (decoder.nextFile()) {
                lineNumber = 0;

                while ((line = decoder.poll()) != null) {
                    lineNumber++;
                    linePos = 0;

                    // parse time
                    if (timeIndex.isPresent()) {
                        int i = line.indexOf(task.getDelimiterChar(), linePos);
                        String value = line.substring(linePos, i);
                        linePos = i + 1;

                        Column column = schema.getColumn(timeIndex.get());
                        Timestamp timestamp = timestampParser.get().parse(value);
                        pageBuilder.setTimestamp(column, timestamp);
                    }

                    // parse tag
                    if (tagIndex.isPresent()) {
                        int i = line.indexOf(task.getDelimiterChar(), linePos);
                        String value = line.substring(linePos, i);
                        linePos = i + 1;

                        Column column = schema.getColumn(tagIndex.get());
                        pageBuilder.setString(column, value);
                    }

                    // parse record
                    Column column = schema.getColumn(recordIndex);
                    Value value = jsonParser.parse(line.substring(linePos));
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
