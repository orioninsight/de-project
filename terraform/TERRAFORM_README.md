## Terraform deployment

1. Setup personal sandbox
   
2. AWS configure
   
3. cd into terraform folder
   
4. Give executable permissions to backend.sh
   ```bash
   chmod u+x backend.sh
   ```

5. Run backend.sh and note created bucket from output.
   
6. In backend.tf change bucket to equal the name of your bucket created by backend.sh e.g.
    ```
    terraform {
    backend "s3" {
    bucket = "nc-terraform-state-1679486354"
    key    = "tote-application/terraform.tfstate"
    region = "us-east-1"
      }
    }
    ```

7. Run:
   ```bash
   terraform init
   ```

8. Variables that can be changed are the lambda handler
   - extraction_lambda_handler should match the defined handler function e.g. extract_db.extract_db_handler

9. extraction lambda files should reside in src/extraction_lambda/

10. Run:
 ```bash
   terraform plan
   terraform apply
   ```