# Automating_Data_Pipelines_with_Airflow

This project is a continuation of a previous project on [cloud data warehouse of a company's data in Amazon Redshift](https://gitlab.com/offor20/aws_cloud_data_warehouse). The firm decides to automate its ETL data pipelines in AWS Redshift.For this reason, the project implements automation and monitoring of the company's data warehouse ETL pipelines using [Apache Airflow](https://airflow.apache.org/). The datasets located in AWS S3, would be fetched, analyzed, and processed in the company's data warehouse in Amazon Redshift autonomously using Apache Airfflow. 

Apache airflow is designed to be scalable, dynamic, elegant and extensible [1](https://airflow.apache.org/). As such, this project is based on the airflow core features and has incorporated a number of customized plugins to implement different and specific tasks or components of the project.Also, airflow automates and monitors tasks through the use of directed acyclic graphs (DAGs). A DAG begins with a certain task and ends with a different task with no tasks ever repeated. The DAG implemented in this project is as shown below with all the tasks clearly represented in the rectangular boxes. 

![Project DAG](/images/dag3.png)


## Getting Started
The requirements for the implementation and testing of the project are shown below.
### Prerequisites and installation guides
The following packages and accounts are essential to have the project up and functional:
* Python -> which can be installed as follows:
    * [Python 3 on MacOS.](https://docs.python-guide.org/starting/install3/osx/#install3-osx)
    * [Python 3 on Linux.](https://docs.python-guide.org/starting/install3/linux/#install3-linux)
    * [Python 3 on Windows.](https://docs.python-guide.org/starting/install3/win/#install3-windows)
* Follow the guidelines [here to install Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html)
* [Create or sign in to an AWS account (My Account) here](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/)
* [Create I AM User here](https://console.aws.amazon.com/iam/) by choosing **Users**. Under access type use: **Programmatic access**.
* [Launch an AWS Redshift Cluster](https://console.aws.amazon.com/redshift/) by filling out the necessary configuration details such as the following:
    * Redshift Identifier
    * Type and number of nodes
    * Select free trial for learning and development
    * Database necessary details are:
        * Database Name
        * Database Port which has to be 5439
        * Master user name
        * Master user password

***It is advisable to delete a cluster after use to avoid exorbitant bills*** 

## Implementation Steps
The following steps are carried out in the implementation of the project's concepts:

1. The project DAG: As stated above that airflow automates and monitors tasks by building Directed Acyclic Graphs (DAGs), the project dag is implemented in python codes, specifically in a file called dag.py inside the dag folder. The project dag consists of eleven tasks with each task scoped differently with specific goal in mind. The dag.py file defines the tasks dependencies as shown in the diagram above. The diagram shows that some task without any dependencies are parallelized to run concurrently such as Stage_events and Stage_songs. All the tasks with the exception of Begin_execution, create_tables and End_execution use customized operators as plugins. Begin_execution and End_execution tasks make use of DummyOperator while create_tables task uses PostresOperator to create tables in the Redshift cluster.
1. The customized operators defined in the project are located in an operators folder inside plugins folder. There are four customized operators, namely: 
    * **DataQualityOperator** inside data_quality.py file: This operator with its associated task checks the data integrity of the tables created and populated with data ensuring that no table is empty and that there no NULL Values where necessary.
    * **StageToRedshiftOperator** inside stage_redshift.py file with its associated tasks Stage_events and Stage_songs implements copying of the datasets from S3 to the Redshift.
    * **LoadFactOperator** inside the load_fact.py file loads data into the fact (songplays) table.
    * **LoadDimensionOperator** inside the load_dimension.py file with its respective tasks loads data into the four dimensional tables. 
1. The create_tables task runs the create_tables.sql file to create all the tables. All other SQL codes are defined in the sql_queries.py file inside a helpers folder. In the file is a class called SqlQueries. 

1. In the dag.py file is all the operators and helper sql codes imported. A DAG is also imported from airflow and is used to define project DAG with different parameters such as dag's title (project_dag), defualt arguments, description of the dag, schedule_interval which is monthly because the data is presumed to have arrived monthly, among others. All the tasks are then defined using the different aforementioned operators.  

## Running the Project and Visualizing the DAG
Prior to running the DAG, launch the Redshift cluster and ensure the cluster has a status **available**. 

1. Open the project in the terminal and enter
```
sudo docker-compose up

``` 
2. Wait until the terminal reads .. exited with 0.

3. Open a web browser and go to [http://localhost:8080](http://localhost:8080). Login with user name **airflow** and password **airflow**.
4. The taskbar, click on Admin and then connections. Click on create to create new connection. The project requires two connections to be created as follows:

*  Click "Create" and set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services". Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key. Click save. 

* For the second connection, click "Create" and set "Conn Id" to "redshift", "Conn Type" to "Postgres", set "Host" to the link you get by clicking on your Redshift cluster excluding the port number and colon, set the "schema" to the exact database name you used when creating the cluster,set "Login" to your master username and "Password" to your master password. Click save. 

5.  Click on the DAG. You will see the **project_dag**. Toggle it on and trigger the dag or refresh the browser to run the dag. The UI interface can be used to have a graphical view of the project by clicking on the "Graph View". Each Task logs can be viewd as well. 

## Authors
Ernest Offor Ugwoke - previous work on [data lake in amazon EMR ](https://gitlab.com/offor20/data_lake_in_aws_emr)

## Acknowledgement
The author greatly acknowledges the[Udacity data team](www.udacity.com) for their guidance, supervision and supports.

