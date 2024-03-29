# ETL in Data Lakes

## Abstract

Sparkify decided to replicate its data pipline using Spark and AWS, that will allow them to stream large datasets of songs.
Therefore we decided create Schema for Song Play Analysis Using the song and log datasets, by creating a star schema consist of 
one fact table songplays and 4 dimension tables, users, time, songs and artists. 


## How We Processed the Data?

Data have been read from the public S3 bucket of Udacity, using Spark we read the data while joining the different directories using os.path.join function 

Reading the data might take longer, one way to fix this issue can be by creating the parsing the data with structured streaming, however specifying one path can reduce the weight of data to be read and speed the performance. 

Next, wranggling data using PYSPARK that will allow us to extract columns to create different tables. You can find that we read data as json, filtered those data and ectracted the time date from timestamp and wrote it back as a parquet files stored into S3 bucket in our own AWS account. Concluding this process with the FACT TABLE (Songplay Table) that will join the log_data along with song_data using title, duration, and artist name to be written again as a table in parquet back to the S3 Bucket. 

## Project Parts
1. **dl.cfg:** the amazon configuration file that holds the creds of the AWS account which will hold the output bucket to write into.
2. **etly.py:** Holds the actual logic of reading from the public udacity S3 bucket, wrangling the data and writting the output to your own AWS bucket

*Note: you will notice a directory will get created everytime you run the file and that ensures your code is working and the output files are being written into the s3 directory.*

## Sparkify Execution 

<code> python3 etly.py </code>

This will trigger the initalization of all the imports ou defined in the file , spark, the functions , they pyspark, the config parser then it will be reading the json and processing it on parts, after that you will see the data is being written on a small directory in the 
