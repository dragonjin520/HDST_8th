# 학습 목표
*** This is 1st part of W3M2.
Demonstrate your understanding of Apache Hadoop by setting up a multi-node Hadoop cluster using Docker. This project will help you gain hands-on experience with Hadoop cluster configuration and Docker containerization.

## 기능요구사항
### Docker Images:
The Docker images should encapsulate fully functional Hadoop master and worker nodes.

When the Docker containers run, they should automatically start all necessary Hadoop services.

The containers should be able to connect to the same Docker network to facilitate communication between master and worker nodes.

### HDFS Operations:
Users should be able to interact with HDFS from the master node.

Users should be able to create directories, upload files, and retrieve files from HDFS.

The HDFS web interface should be accessible from the host machine to monitor the file system.

### Cluster Operations:
The Hadoop cluster should recognize and utilize the worker nodes for distributed storage and processing.

YARN ResourceManager should distribute tasks to NodeManagers running on worker nodes.

The cluster should be able to run a sample MapReduce job successfully, demonstrating distributed processing.

### Persistence:
The Hadoop data directory within the Docker containers should be configured to persist data between container restarts.

Ensure that the data stored in HDFS remains intact even if the containers are stopped and restarted.

### Documentation:
Provide clear instructions on how to build the Docker images and run the containers.

Include steps for configuring Hadoop within the containers and starting the services.

Document how to perform basic HDFS operations, such as creating directories, uploading files, running MapReduce jobs, and retrieving files.

## 프로그래밍 요구사항
### Docker Setup:
Install Docker on your local machine if it is not already installed.

Create Dockerfiles for configuring the Hadoop environment for multiple nodes.

Build Docker images from the Dockerfiles to set up a Hadoop master node and at least one Hadoop worker node.

### Hadoop Configuration:
Configure Hadoop core-site.xml, hdfs-site.xml, mapred-site.xml, and yarn-site.xml files for a multi-node cluster.

Set up Hadoop environment variables for both the master and worker nodes.

Format the HDFS namenode on the master node.

### Network Configuration:
Ensure that the Docker containers can communicate with each other by setting up a Docker network.

Configure the Hadoop cluster such that the master node recognizes and can communicate with the worker node(s).

### Start Hadoop Services:
Start the Hadoop NameNode service on the master node and the Hadoop DataNode service on each worker node.

Ensure that HDFS is running correctly across all nodes.

Start YARN ResourceManager and NodeManager services on the respective nodes.

### Data Operations:
Create a directory in HDFS.

Upload a sample file from the local file system to HDFS.

Retrieve the file from HDFS to ensure it was uploaded successfully.

Run a MapReduce job on the Hadoop cluster to validate its functionality.

## 예상결과 및 동작예시
### Running Containers:
Running Docker containers with a Hadoop master node and at least one worker node.

All Hadoop services (namenode, datanode, ResourceManager, NodeManager, etc.) should be up and running within the containers.

### HDFS Operations:
The ability to create a directory in HDFS from the master node.

Successfully upload a file from the local file system to HDFS.

Retrieve the uploaded file from HDFS to the local file system.

### Cluster Operations:
The ability to run a sample MapReduce job on the Hadoop cluster.

Verify that the job utilizes both the master and worker nodes for processing.

### Accessibility:
Access the HDFS and YARN web interfaces from the host machine to verify the cluster's status and perform file system and job monitoring operations.

# Submission:
Submit the Dockerfiles and any other configuration files used for setting up the Hadoop cluster.

Provide a README file with step-by-step instructions for building the Docker images, running the containers, and performing HDFS and MapReduce operations.