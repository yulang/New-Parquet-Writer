#New Parquet Writer


Parquet CSV writer compatible with Parquet 1.8.

## Execution

To use the writer, run mvn test -Dtest=NewWriter on parquet-writer-1.0.0/ directory.

Write your main function at parquet-writer-1.0.0/src/test/java/parquet/writer/test/NewWriter.java with @test

Core function - convertCsvToParquet( )

###Argument list:

* File csvFile - Input CSV file. By default, use "|" as delimiter. It can be configured in NewWriter.java.
* File outputParquetFile - Output Parquet file.


###Schema

A file of EXACT THE SAME name of the input csvFile with .schema extension must exist in the same directory with cvs file to specify the schema of the data. In the schema file, few things need to be declared:

* Optional/Required/Repeated of each fields
* Data type of each fields
* Field name

An example:

```
message m {
	required int32 id;
}

```
###TODO

* Support for FIXED_LEN_BYTE_ARRAY type
