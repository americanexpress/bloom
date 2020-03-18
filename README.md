
#  BLooM - Bulk Loader of MemSQL 

![BLooM](images/bloom.png) 

## BLooM is a configuration driven bigdata based framework to load massive data into [MemSQL Database](https://www.memsql.com/).

### Features -->

- Blazing performance 50 Millions records loaded in [MemSQL Database](https://www.memsql.com/) in 90 Seconds leveraging [Apache Spark](https://spark.apache.org/).
- It can load data in all three types of tables in MemSQL - **Columnstore, Rowstore and Reference Table**.
- Supports **control-A and comma delimited files** as input.
- Also support **data load from Hive table** directly to MemSQL.
- supports processing multiple files in a directory.
- There are three **types of modes** to run the framework:    
    - **Full load (full refresh)**: It does not check if any records already exists in MemSQL and it just directly loads all the data and overwrites the old data if any. In case the data file has records for only a few columns of the table, the remaining columns are loaded as null.
    - **Delta load (upsert)**: It checks if the record already exists in the MemSQL and if the incoming record is later than the existing one, based on last updated timestamp, then it only makes an upsert, else it ignores the update. For a Delta Load also, the input file can have data for all the columns or for a few columns only. In-case the input has data only for a few columns and the same record is not present already, it inserts the incoming columns and null for the remaining columns. In-case the record already exists but is older, it updates the incoming columns.
    - **Load Append**: It does not check if any records already exists in MemSQL and just appends the incoming data to the existing data. This mode is only supported for columnstore tables.

- It can accept config yaml about an input file / hive table from where data needs to be loaded in MemSQL
- Input can be specified at command line, if its a full load or a delta load
- It is mandatory for the MemSQL table to have a lastModifiedTimeStamp column, because this column is used for the delta load to verify which is the latest record
- Full load does not check if any records already exists in MemSQL and just directly loads all data, pls ensure table is truncated before running full load.
- For a full load the input file can have data for all the columns or for a few columns only, In-case the input has data of only for few columns it updates the remaining columns with null
- Delta load checks if the record already exists in the MemSQL and if the record is latest than the one which exists in DB (based on last updated timestamp) then only it makes an upsert else ignores the update if input record is older
- For a Delta Load, the input file can have data for all the columns or for a few columns only, In-case the input has data of only for few columns and the same record is not present already ,it insert the incoming columns and null for the remaining columns. And in-case the record already exists and but is older it updates the incoming columns but keeps the other column intact

### Prerequisites: 
-  In the case of the input is a CSV file, it should have proper column headers. The order of the header names need not be same to the MemSQL table column order. Also, the csv can have data related to a few columns or all the columns.
-  For the MemSQL table where the data has to be loaded, it is mandatory to have a last updated timestamp column.
-  The utility supports all the data types for MemSQL. While configuring the data types in MemSQL, the user should check whether the incoming data size is greater than the size of the configured data type, because if so, MemSQL will downcast it to the closest value.
-  The input csv file should have unique record for all primary key(s)
-  Note that for Columnstore tables, it is mandatory to have a staging table which will be used during the upsert. The incoming data is first loaded in the staging table, which is then used for comparison with the existing data to come up with the net insert data, which is finally written back to the original table.


### [How to run BLooM](bloom-core/BLooM.md)


### Performance Stats:

| Scenario                 | Table Type | Records Processed |  Run time         | Executors Count | Mode of Processing                | Executors Memory | Executor Core |
|--------------------------|------------|-----------------------------|------------------------|---------------------------|--------------------|------------------------------------|--------------------|
| Fresh Load               | Rowstore   | 5 million                   |  45 seconds            | 100                       | FULL-REFRESH       | 8G                                 | 8                  |
| Fresh Load               | Rowstore   | 5 million                   |  57 seconds            | 10                        | FULL-REFRESH       | 8G                                 | 8                  |
| Fresh Load               | Rowstore   | 50 million                  |  93 seconds            | 100                       | FULL-REFRESH       | 8G                                 | 8                  |
| 50m data changed         | Rowstore   | 50 million                  |  37 min and 37 seconds | 100                       | UPSERT             | 22G                                | 8                  |
| 25 million data changed  | Rowstore   | 50 million                  |  24 min and 39 seconds | 100                       | UPSERT             | 8G                                 | 8                  |
| 2.5 million data changed | Rowstore   | 5 million                   |  179 seconds           | 100                       | UPSERT             | 8G                                 | 8                  |
| 2.5 million data changed | Rowstore   | 5 million                   |  9 min and 56 seconds  | 10                        | UPSERT             | 8G                                 | 8                  |

## Contributing

We welcome Your interest in the American Express Open Source Community on Github. Any Contributor to
any Open Source Project managed by the American Express Open Source Community must accept and sign
an Agreement indicating agreement to the terms below. Except for the rights granted in this 
Agreement to American Express and to recipients of software distributed by American Express, You
reserve all right, title, and interest, if any, in and to Your Contributions. Please
[fill out the Agreement](https://cla-assistant.io/americanexpress/bloom).

## License
Any contributions made under this project will be governed by the
[Apache License 2.0](./LICENSE.txt).


## Code of Conduct
This project adheres to the [American Express Community Guidelines](./CODE_OF_CONDUCT.md). 
By participating, you are expected to honor these guidelines.