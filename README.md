# Distribution Comparison

This repo provides methods of measuring how two empirical distributions differ.

## Quickstart

- Download the latest version of D-SHIFT in <a href="https://github.com/albertusk95/distribution-shift/releases">releases</a> tab
- Create configuration files. See the <a href="https://github.com/albertusk95/distribution-shift/blob/master/src/main/resources/example_config.json">example</a>.
- Run with `java -cp <application_jar> stats.EvaluateDistribution <path_to_config_a> <path_to_config_b> ...`

## Dependencies

- Scala 2.11
- Spark 2.4.4
- circe (JSON library for Scala)

## Current Features

Each method reports its corresponding statistic
- Two-sample Kolmogorov-Smirnov test => max KS distance
- Kullback–Leibler divergence => how much information loss when approaching origin with current distribution

## Possible Next Features

- Other distribution comparison methods
- Test of significance

## Configuration

Take a look at the following config.

```
{
  "eval_method": "kstest",
  "compared_col": {
    "origin_sample_column": "",
    "current_sample_column": ""
  },
  "source": {
    "format": "",
    "path_to_origin_sample": "",
    "path_to_current_sample": ""
  },
  "options": {
    "method": "",
    "numOfBin": null,
    "rounding": null
  }
}
```

### eval_method

- The method used to evaluate how two empirical distributions differ
- Currently supported methods:
    - Two-sample KS test
    - KL divergence

### compared_col

- This field denotes the column (numeric) whose distribution will be compared
- Since each sample data might have different column name for the same data (e.g. `Sex` in first sample & `Gender` in second sample), this field is introduced
- This field consists of two sub-fields, namely `origin_sample_column` and `current_sample_column`
   - `origin_sample_column`: column from sample with expected distribution (e.g. stored data that becomes the base distribution)
   - `current_sample_column`: column from sample with actual distribution (e.g. new data with the same or different distribution with the stored data)

### source

- This field denotes the data sources properties
- The sub-fields include the following:
    - `format`: currently supports `csv` and `parquet` file
    - `path_to_origin_sample`: path to the origin sample set
    - `path_to_current_sample`: path to the current sample set
    
### options

- This field denotes several treatments for internal computation
   - `method`: whether to discretize the data (`binned`) or not (`normal`)
   - `numOfBin`: number of bins when the selected method is `binned`
   - `rounding`: number of digits after comma
    
## Contribute

- PRs are welcome!
- You may add other distribution comparison methods
- You also may add a feature for test of significance
- Bug fixes and features request
