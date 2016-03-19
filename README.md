# Fluentd Out File parser plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **delimiter**: description (integer, required)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: fluentd_out_file
    option1: example1
    option2: example2
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
