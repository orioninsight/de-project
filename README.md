# de-project
<!-- install following before running the file -->
python -m venv venv;
source /venv/activate/bin;
pip install boto3;
pip install pg8000;
pip install pytest;
pip install 'moto[s3, secretsmanager, lambda, cloudwatch, eventsbridge]';