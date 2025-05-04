# project_sber_data_engineering
I've been practicing on user data on using the Sberautopodisc service/"СберАвтоподписка"

 INSTALLATION NOTE: 
******The project, with the exception of 'airfow_docker_pr', must be located in the 'project_sbr' directory.
******For the DAG to work correctly, install the libraries in scheduler and worker before launching. 
******There is a file in the project for this 'requirements.txt'.
******The 'airfow_docker_pr' directory has a 'docker-compose.yaml' on which the operation of airflow is fully implemented. 
******The whole process is implemented on a 'Celery Executor' with a single 'worker' workflow.
******Depending on the amount of data, you need to change the amount of RAM used for docker containers. 
******There is a 'Dockerfile' file for this. This is due to caching information in docker.
******

DIRECTORIES:
******'dags'- tores DAG's files
******'data'- stores and processes old and new data. Basic: 'data_old', 'data_new'. The rest are used during operation/
******'jupyter_notebooks'- Stores information on internal data processing and analytics.
******'logs'- Stores information with DAG's logs
******'modules'- Ыtores modules used by DAG's for data processing
