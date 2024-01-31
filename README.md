# Data-Career-Portfolio

## Projects
### [ETL] GCP Data Engineer Project
In this project, I designed and implemented an ETL data pipeline using Google Cloud Storage as Data Lake, Google BigQuery as Data Warehouse and Google Cloud Composer for runing Apache Airflow as Data Orchestrator.
This system is running on Google Cloud Platform.
* Technology used : Google Cloud Storage, Google BigQuery,  Airflow, Looker Studio.
* Architechture Diagram :

  ![Diagram](https://github.com/Younive/Data-Career-Portfolio/blob/main/GCP-Data_Engineer-Project/GCP_pipeline_diagram.jpg)
  
* Dashboard : [Audible Sale Dashboard](https://lookerstudio.google.com/reporting/848e065d-171a-4f3f-8c79-06672c286890)

---

### [ELT] Retail Data Engineer Project
In this project, I designed and implemented an ELT data pipeline, leveraging Google Cloud Storage as a robust Data Lake, Google BigQuery as a high-performance Data Warehouse, and Apache Airflow as the orchestrator. The entire system is seamlessly orchestrated locally through the Astro CLI.
* Technology used: Google Cloud Storage, Google BigQuery,  Airflow, Looker Studio, DBT Core, Docker
* Architechture Diagram :

  ![Diagram](https://github.com/Younive/Data-Career-Portfolio/blob/main/retail_de_project/images/elt_diagram.png)
  
* Dashboard : [Retail Dashboard](https://lookerstudio.google.com/reporting/381987ec-9e6f-45ed-91b3-747c6375df3c)

  ![Retail Dashboard](https://github.com/Younive/Data-Career-Portfolio/blob/main/retail_de_project/images/dashboard.png)

---

### [Streaming] Weather Monitoring Stream Data Pipeline
The extraction process is done using  Kafka, The data is streamed from the OpenWeatherMap API followed by creation of topics and publishing using Apache Kafka.
In the transformation and load process, schema is extracted from the stream of data from API and reading of data from apache Kafka as streaming a dataframe. Then, data will be written in Cassandra for further data usage.

* Technology used: Apache Kafka, Apache Spark, Cassandra, Docker
* Architechture Diagram :

  ![Diagram](https://github.com/Younive/Data-Career-Portfolio/blob/main/weather_monitoring/images/architechture.png)
  
* Cassandra :

![cassandra](https://github.com/Younive/Data-Career-Portfolio/blob/main/weather_monitoring/images/sample_cassandra.png)

---

### [ETL] HADOOP Data Pipeline

* Architechture Diagram :

  ![Diagram](https://github.com/Younive/Data-Career-Portfolio/blob/main/hadoop_data_pipeline/media/hadoop_pipeline_architechture.png)

* Next step development : - replace Flume with Kafka

---

### [ML] Sentiment Analysis Machine Learning Model
- This project is developed in SparkML, using Amazon sports and outdoors products review as a dataset.
- There's the overall score which has a score range 1.0 to 5.0. I dropped rows  where the overall score is 3.0 due to ambiguous sentiment, however, there's more than enough data for training and testing.
- Created the pipeline to train the model, there are 5 stages including tokenizing, removing stop word, CountVectorization and Logistic regression.
- The model was evaluated using a Binary Classification Evaluator on a total of 9003 rows of test data. The results indicate that the model correctly predicted 7776 rows, while 1227 rows were predicted incorrectly.

---
### [MLE] Fast ML API

#### Deploy Machine Learining model as an ML API using FastAPI

---

### Conizant Data Scientist Job Simulate
This virtual internship as Data Scientist, I..
- Completed a job simulation focused on AI for Cognizant’s Data Science team.
- Conducted exploratory data analysis using Python script and Python notebook  for one of Cognizant’s technology-led clients, Gala Groceries.
- Prepared a Python module that contains code to train a model and output the performance metrics for the Machine Learning engineering team.
- Communicated findings and analysis in the form of a PowerPoint slide to  present the results back to the business.

---



### Simple LLM Chatbot
I developed LLM Chatbot using Langchain Framework and OpenAI as based LLM model. The chatbot can answer online course questions by understanding the context of the question and matching it with prepared prompts and answers.
* Technology used: Langchain Framework, OpenAI API, Faiss Vector Database, Streamlit
