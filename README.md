# Glue Overwrite Table Sample in CDK

## Overview
This code explains how minimize downtime when overriding the data of a Glue CatalogTable. 

ETL processes will usually rely on two techniques to write data: Append or Overwrite. Append mode will add the input data towards the target table and Override Processes will overwrite the target table contents. 

Hadoop based processes will often rely on appending to outputs, but if you need to overwrite a target table without downtime to readers, Glue and Spark standard behavior is to delete the target table’s data and then start writing the data to the target, which can sometimes take a long time and leave data consumers waiting. This article will detail how can you achieve a reduction of that downtime using a temporary table and a replacement the updata-table command to recreate the metadata of the target table essentially pointing the existing table storage location to a new path.

The technique is able to bring the downtime to 0 on unpartitioned tables and to bring it to a second’s scale when dealing with partitioned tables.

## Walkthrough
*Important: This walkthrough will use CDK to deploy resources into the target account, including a service role, a Glue Job and a Table. Do not run this code in a production environment.*

To demonstrate the table replacement in action we will create a CloudFormation stack containing:

- A Glue Service role to run the workflow that will overwrite the target table
- An S3 bucket that will contain:the script code for the Job that will overwrite the table of the database that will contain the table being overwritten 
- An external table, pointing an Ookla’s public open dataset in AWS Data Registry. This table is partitioned and stored in parquet format 
- A Job that will be executed to overwrite the target table in the Glue Catalog

Finally, we will be able to test the access to the table by performing a select query in Athena during the table write to ensure downtime is minimized

## How to use

After that, navigate to the checked-out repo and run the cdk command below to deploy the stack using Cloud Formation through CDK:
```
➜ cdk deploy
```

Let’s perform the overwrite now. Navigate to the Glue Console and start the job glue_overwrite_table_sample_glue_job_cdk. This job will read the source table we identified on step 6 and re-write it to the target table glue_overwrite_table_sample_output_glue_job_table, it normally takes around 5 minutes to execute. You can validate the creation of the new table in Athena and check the data on S3.

Finally run the script again and check the target table glue_overwrite_table_sample_output_glue_job_table can still be queried while the data is being written. If you verify in Athena and S3 you’ll be able to verify the temporary data being written on a separate directory. After the job is completed, the existing table will be updated to point to the new location. 

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
