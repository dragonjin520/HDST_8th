학습 목표
Demonstrate your understanding of Apache Hadoop and MapReduce by writing a MapReduce job in Python to analyze the sentiment of tweets using the 'Twitter Data from Sentiment140' dataset. This project will help you gain hands-on experience with Hadoop's MapReduce framework by building a sentiment analysis program and running it on a large dataset.

기능요구사항
Data Processing:
The program should correctly read input data from HDFS.

The Mapper should process each tweet and emit relevant key-value pairs for sentiment classification.

The Reducer should aggregate these key-value pairs and output the total counts for each sentiment category.

Job Execution:
The MapReduce job should run without errors on the Hadoop cluster.

The job should efficiently process the input data and produce accurate results.

Result Output:
The output should be a text file stored in HDFS.

Each line of the output file should contain a sentiment category and its corresponding count, separated by a tab or space.

The output should be retrievable and readable from HDFS.

프로그래밍 요구사항
Dataset:
Use the 'Twitter Data from Sentiment140' dataset. Ensure you have downloaded and uploaded it to the Hadoop Distributed File System (HDFS).

MapReduce Program:
Write a MapReduce job in Python to classify the sentiment of tweets as positive, negative, or neutral.

Implement the Mapper and Reducer classes:

Mapper:
The Mapper should process each tweet and classify it as positive, negative, or neutral based on predefined keywords.

Emit key-value pairs where the key is the sentiment category ('positive', 'negative', 'neutral') and the value is 1.

Reducer:
The Reducer should aggregate the counts for each sentiment category.

Output the total count for each sentiment category.

Running the MapReduce Job:
Package your MapReduce program into a script that can be executed on the Hadoop cluster.

Submit the MapReduce job to the Hadoop cluster.

Monitor the job's progress through the Hadoop web interface or command-line tools.

Result Collection:
Retrieve the output of the MapReduce job from HDFS.

Ensure the output contains the counts for each sentiment category.

예상결과 및 동작예시
Example Expected Outcome:
The output should be a text file in HDFS with the following format:

positive   1500
negative   1200
neutral    1800
Documentation:
Provide clear instructions on how to compile and run the MapReduce program.

Include steps for uploading the input data to HDFS and retrieving the output data.

Document how to interpret the output and verify the results.

Submission:
Submit the source code for the MapReduce program.

Provide a README file with step-by-step instructions for compiling the program, uploading the input data to HDFS, running the MapReduce job, and retrieving the output data.

팀 활동 요구사항
'predefined keywords'가 아닌 다른 방법으로 나누고자 한다면 어떤 방법이 있을까요? 팀과 함께 논의해서 다른 방법을 시도해 보세요.