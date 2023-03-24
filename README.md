# de-project
<!-- install following before running the file -->
## Install
```
python -m venv venv;
source /venv/bin/activate
pip install boto3;
pip install pg8000;
pip install pytest;
pip install 'moto[s3, secretsmanager, lambda, cloudwatch, eventsbridge]';
pip install pandas;
```
Set environment variable OI_TOTESYS_SECRET_STRING to '{"host":"HOST","port":"PORT","user":"USER","password":"PASSWORD","database":"DB"}', replacing values as necessary. 

***DON'T FORGET THE SINGLE QUOTES!***


## Run tests
Use a command like the following:
```
clear; export PYTHONPATH=$(pwd); pytest -vrP test/extraction/test-saver.py
```

## Run extraction lambda manually
To run the extraction lambda 'extract_db' manually, add a secret with ID 'totesys' to AWS Secrets Manager
