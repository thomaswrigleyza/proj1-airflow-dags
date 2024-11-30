# proj1-airflow-dags

**Key facts**
This repo stores Airflow DAGs for Project #1, which demonstrates an end-to-end data solution, from data source to visualisation.
These DAGs draw from OpenWeather's API and the World Bank's API. This Airflow instance runs on GCP's Cloud Composer solution.

**Project #1 background**
I struggle with my sinuses, particularly during changes of season. On a windy November day, a question formed in my mind: What relationship exists between air quality and key economic and political metrics? Many confounding variables are likely at play - geography, economic development, level and type of industry and so forth. But are there any overarching patterns? Do metrics like government effectiveness, level of corruption and related metrics have any impact on air quality?

Project #1 is an investigation of these questions, as well as a demonstration of an end-to-end data solution. Data is sourced from OpenWeather's API and the World Bank's API, extracted and loaded by Airflow into a BigQuery data warehouse. These data are transformed into staging and mart tables by dbt Cloud. Visualisations are rendered in Looker Studio.

See https://www.thomaswrigley.com/proj2 for more detail.

