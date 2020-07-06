# Distribution Comparison

This repo provides methods of measuring how two distributions differ.

## Quickstart

- Download the latest version of D-SHIFT in <a href="https://github.com/albertusk95/distribution-shift/releases">releases</a> tab
- Create configuration files. See the <a href="https://github.com/albertusk95/distribution-shift/blob/master/src/main/resources/example_config.json">example</a>.
- Run with `java -cp <application_jar> stats.EvaluateDistribution <path_to_config_a> <path_to_config_b> ...`

## Dependencies

- Scala 2.11
- Spark 2.4.4
- circe (JSON library for Scala)

## Current Features

- Two-sample Kolmogorov-Smirnov test
    - Currently, this test only reports the D-statistic without the test of significance

## Possible Next Features

- Other distribution comparison methods, such as KL divergence
- Test of significance

## Configuration

Take a look at the following config.

```
{
  "eval_method": "kstest",
  "compared_col": {
    "sample_one_column": "<col in first sample to be compared>",
    "sample_two_column": "<col in second sample to be compared>"
  },
  "source": {
    "format": "<file format>",
    "path_to_first_sample": "<path to first sample dataset>",
    "path_to_second_sample": "<path to second sample dataset>"
  }
}
```

### eval_method

- The method used to evaluate how two distributions differ
- Currently supported methods:
    - Two-sample KS test

### compared_col

- This field denotes the column (numeric) whose distribution will be compared
- Since each sample data might have different column name for the same data (e.g. `Sex` in first sample & `Gender` in second sample), this field is introduced
- This field consists of two sub-fields, namely `sample_one_column` and `sample_two_column`. It should be self-explanatory

### source

- This field denotes the data sources properties
- The sub-fields include the following:
    - `format`: currently supports `csv` and `parquet` file
    - `path_to_first_sample`: path to the first sample dataset
    - `path_to_second_sample`: path to the second sample dataset
    
## Contribute

- PRs are welcome!
- You may add other distribution comparison methods, such as KL divergence
- You also may add a feature for test of significance
- Bug fixes and features request
