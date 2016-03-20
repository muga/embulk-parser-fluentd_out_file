# Fluentd Out File parser plugin for Embulk

This plugin parses fluentd's out_file formatted files.
http://docs.fluentd.org/articles/out_file

## Overview

* **Plugin type**: parser
* **Guess supported**: yes

## Configuration

- **delimiter**: Delimiter character such as \t (string, required)
- **columns**: Columns (hash, required)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: fluentd_out_file
    delimiter: "\t"
    columns:
    - {name: time, type: timestamp, format: '%Y-%m-%dT%H:%M:%S%:z'}
    - {name: tag, type: string}
    - {name: record, type: json}
```

(If guess supported) you don't have to write `parser:` section in the configuration file. After writing `in:` section, you can let embulk guess `parser:` section using this command:

```
$ embulk gem install embulk-parser-fluentd_out_file
$ embulk guess -g fluentd_out_file config.yml -o guessed.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Test

```
$ ./gradlew clean test jacocoTestReport
```