#!/bin/bash
rm -Rf astro
mkdir astro

cd astro
astro dev init
cd ..
cp airflow_settings.yaml astro/
cp -R dags/* astro/dags/
cd astro
cat >> requirements.txt << EOF
boto3
EOF
astro dev start
astro dev object import