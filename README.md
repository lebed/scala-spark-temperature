## Weather analyzer

[![Build Status](https://travis-ci.org/lebed/scala-spark-temperature.svg?branch=master)](https://travis-ci.org/lebed/scala-spark-temperature)

#### Implementing methods for analyzing the climate using meteorological data on Scala and Apache Spark(DS/SQL).

The data for this project was taken from Google Public Data sets.

##### The following questions are implemented in this project:

- What is the min/average/high temperature per month from hottest month to coldest month for all data? :white_check_mark:
- What is the min/average/high temperature per month for country and state ordered by measurements? :white_check_mark:
- Show all records with the maximum temperature for every month. :white_check_mark:
- How many days were temperatures above 75ºF? :white_check_mark:
- Show all available countries grouped by state :white_check_mark:

This project requires sbt, JDK 8 and scala 2.12 and spark 2.4.2

| Can Reorder | 2nd operation |2nd operation |2nd operation |
| :---: | --- |
|1st operation|Normal Load <br/>Normal Store| Volatile Load <br/>MonitorEnter|Volatile Store<br/> MonitorExit|
|Normal Load <br/> Normal Store| | | No|
|Volatile Load <br/> MonitorEnter| No|No|No|
|Volatile store <br/> MonitorExit| | No|No|
