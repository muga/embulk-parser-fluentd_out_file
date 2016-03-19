module Embulk
  module Guess
    require 'embulk/guess/schema_guess'

    class FluentdOutFileGuessPlugin < LineGuessPlugin
      Plugin.register_guess("fluentd_out_file", self)

      DELIMITER_CANDIDATES = [
        "\t", ",", "|"
      ]

      def guess_lines(config, sample_lines)
        return {} unless config.fetch("parser", {}).fetch("type", "fluentd_out_file") == "fluentd_out_file"

        parser_config = config["parser"] || {}

        # guess delimiter
        if parser_config["type"] == "fluentd_out_file" && parser_config["delimiter"]
          delim = parser_config["delimiter"]
        else
          delim = guess_delimiter(sample_lines)
          unless delim
            # not fluentd_out_file file
            return {}
          end
        end

        parser_guessed = DataSource.new.merge(parser_config).merge({"type" => "fluentd_out_file", "delimiter" => delim})

        # guess schema
        sample_records = sample_lines.map {|line| line.split(delim)}
        column_types = SchemaGuess.types_from_array_records(sample_records || [])
        if column_types.size > 3
          # not fluentd_out_file file
          return {}
        end
        schema = []
        column_types.each do |type|
          if type.is_a?(SchemaGuess::TimestampTypeMatch)
            schema << {"name" => "time", "type" => type, "format" => type.format}
          elsif type == "string"
            schema << {"name" => "tag", "type" => type}
          elsif type == "json"
            schema << {"name" => "record", "type" => type}
          else
            # not fluentd_out_file file
            return {}
          end
        end
        parser_guessed["columns"] = schema

        return {"parser" => parser_guessed}
      end

      private # ported from csv_guess.rb temporarily

      def guess_delimiter(sample_lines)
        delim_weights = DELIMITER_CANDIDATES.map do |d|
          counts = sample_lines.map {|line| line.count(d) }
          total = array_sum(counts)
          if total > 0
            stddev = array_standard_deviation(counts)
            stddev = 0.000000001 if stddev == 0.0
            weight = total / stddev
            [d, weight]
          else
            [nil, 0]
          end
        end

        delim, weight = *delim_weights.sort_by {|d,weight| weight }.last
        if delim != nil && weight > 1
          return delim
        else
          return nil
        end
      end

      def array_sum(array)
        array.inject(0) {|r,i| r += i }
      end

      def array_avg(array)
        array.inject(0.0) {|r,i| r += i } / array.size
      end

      def array_variance(array)
        avg = array_avg(array)
        array.inject(0.0) {|r,i| r += (i - avg) ** 2 } / array.size
      end

      def array_standard_deviation(array)
        Math.sqrt(array_variance(array))
      end
    end
  end
end
