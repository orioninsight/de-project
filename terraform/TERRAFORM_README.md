## Terraform deployment

1. Setup personal sandbox
   
2. AWS configure
   
3. cd into terraform folder
   
4. Give executable permissions to backend.sh
   ```bash
   chmod u+x backend.sh
   ```

5. Run backend.sh which will generate a backend.tf file with the first existing bucket with prefix 'nc-terraform-state-' or generate a new one
   
6. You can verify this in backend.tf and change the bucket name if needed e.g.
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