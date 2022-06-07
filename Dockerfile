FROM apache/airflow

USER root
ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags
# ADD app /opt/airflow/app
# ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/app"

USER airflow
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org boto3