#!/usr/bin/env bash
airflow resetdb
airflow db init
airflow upgradedb
airflow users create -r Admin -u admin -e amitb2050@gmail.com -f Amit -l Baswa -p admin
airflow scheduler &
airflow webserver