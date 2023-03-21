CREATE external TABLE base.us_current(
    `date` int,
    `states` int,
    `positive` int,
    `negative` int,
    `pending` int,
    `hospitalizedcurrently` int,
    `hospitalizedcumulative` int,
    `inicucurrently` int,
    `inicucumulative` int,
    `onventilatorcurrently` int,
    `onventilatorcumulative` int,
    `datechecked` string,
    `death` int,
    `hospitalized` int,
    `totaltestresults` int,
    `lastmodified` string,
    `recovered` string,
    `total` int,
    `posneg` int,
    `deathincrease` int,
    `hospitalizedincrease` int,
    `negativeincrease` int,
    `positiveincrease` int,
    `totaltestresultsincrease` int,
    `hash` string)
ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      ESCAPED BY '\\'
      LINES TERMINATED BY '\n'
LOCATION
  's3://<<code_bucket>>/covid-19-testing-data/base/source_us_current/'
  TBLPROPERTIES ("skip.header.line.count"="1");
