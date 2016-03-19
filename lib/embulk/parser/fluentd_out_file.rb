Embulk::JavaPlugin.register_parser(
  "fluentd_out_file", "org.embulk.parser.fluentd_out_file.FluentdOutFileParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
