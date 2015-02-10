# RCFileTest

Here is an example how to create RCFile using LazyBinaryColumnarSerDe which is a default SerDe in hive 0.12 and above.

## Steps to build:

1. update hardcoded paths in the src/main/java/test/hive/rcfile/RCFileTest.java
2. mvn clean package

## How to run

1. Create table from the hive command line:

        hive> create external table testtable(row string, id bigint) stored as rcfile location '/user/do/testtable';

2. Run RCFileTest as following:

        HIVE_HOME=path_to_the_hive HADOOP_HOME=path_to_the_hadoop ./run

## Check results

From the hive command line:

        hive> select * from testtable;
        LineageState size: 0
        LineageState has been cleared
        OK
        row0	1000000
        row1	1000001
        row2	1000002
        row3	1000003
        row4	1000004
        row5	1000005
        row6	1000006
        row7	1000007
        row8	1000008
        row9	1000009
        row10	1000010
        row11	1000011
        row12	1000012
        row13	1000013
        row14	1000014
        row15	1000015
        row16	1000016
        row17	1000017
        row18	1000018
        row19	1000019
        row20	1000020
        row21	1000021
        row22	1000022
        row23	1000023
        row24	1000024
        row25	1000025
        row26	1000026
        row27	1000027
        row28	1000028
        row29	1000029
        Time taken: 1.27 seconds, Fetched: 30 row(s)
