# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from datetime import datetime

import boto3
from pyspark.context import SparkContext


def get_catalog_table(glue_client, db_name, table_name):
    try:
        return glue_client.get_table(DatabaseName=db_name, Name=table_name)
    except:
        return None


def delete_partitions(glue_client, database, table, batch=25):
    paginator = glue_client.get_paginator('get_partitions')
    response = paginator.paginate(
        DatabaseName=database,
        TableName=table
    )

    for page in response:
        partitions = page['Partitions']

        for i in range(0, len(partitions), batch):
            to_delete = [{k: v[k]} for k, v in zip(["Values"] * batch, partitions[i:i + batch])]
            glue_client.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=to_delete
            )


def copy_partitions(glue_client, source_database, source_table, target_database, target_table):
    paginator = glue_client.get_paginator('get_partitions')
    response = paginator.paginate(
        DatabaseName=source_database,
        TableName=source_table,
        PaginationConfig={'PageSize': 100}
    )

    for page in response:
        partitions = page['Partitions']
        clean_partitions = []

        for part in partitions:
            clean_partition = {key: part[key] for key in part if
                               key not in ['DatabaseName', 'TableName', 'CreationTime']}
            clean_partitions.append(clean_partition)

        glue_client.batch_create_partition(
            DatabaseName=target_database,
            TableName=target_table,
            PartitionInputList=clean_partitions
        )


def get_output_path(glue_client, output_database, output_table):
    response = glue_client.get_database(Name=output_database)
    database_location_uri = response['Database']['LocationUri']
    if database_location_uri.endswith('/'):
        database_location_uri = database_location_uri[:-1]
    return f"{database_location_uri}/{output_table}"


def read_from_catalog(glue_context, source_database, source_table):
    return glue_context.create_dynamic_frame.from_catalog(
        database=source_database,
        table_name=source_table,
        transformation_ctx='source_dynamic_frame'
    )


def write_parquet(glue_context, dyf, output_path, partition_keys, output_database, output_table):
    sink = glue_context.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        partitionKeys=partition_keys
    )
    sink.setFormat(format='glueparquet')
    sink.setCatalogInfo(
        catalogDatabase=output_database,
        catalogTableName=output_table
    )
    sink.writeFrame(dyf)


def write_versioned_parquet(glue_client, glue_context, dyf, output_path, partition_keys, output_database, output_table):
    existing_target_table = get_catalog_table(glue_client, output_database, output_table)

    if existing_target_table is None:
        output_path = f"{output_path}/version_0"
        write_parquet(glue_context, dyf, output_path, partition_keys, output_database, output_table)
    else:
        now = datetime.now()
        version_tmp_suffix = f"_version_tmp_{now.strftime('%Y%m%d%H%M')}"
        version_tmp_table = f"{output_table}{version_tmp_suffix}"
        next_location = calculate_next_location(existing_target_table)

        write_parquet(glue_context, dyf, next_location, partition_keys, output_database, version_tmp_table)

        version_tmp_table_result = get_catalog_table(glue_client, output_database, version_tmp_table)['Table']

        table_input = {key: version_tmp_table_result[key] for key in version_tmp_table_result if
                       key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName',
                                   'IsRegisteredWithLakeFormation']}
        table_input['Name'] = output_table
        table_input['StorageDescriptor']['Location'] = next_location

        delete_partitions(glue_client, output_database, output_table)
        copy_partitions(glue_client, output_database, version_tmp_table, output_database, output_table)

        glue_client.update_table(DatabaseName=output_database, TableInput=table_input)
        glue_client.delete_table(DatabaseName=output_database, Name=version_tmp_table)


def calculate_next_location(existing_target_table):
    curr_table = existing_target_table['Table']
    curr_location_split = curr_table['StorageDescriptor']['Location'].split('/')
    curr_version_suffix = curr_location_split[-2]
    curr_version_int = int(curr_version_suffix.replace('version_', ''))
    next_version_int = curr_version_int + 1
    next_location_split = curr_location_split[:-2] + [f'version_{str(next_version_int)}', '']
    next_location = '/'.join(next_location_split)
    return next_location


def main():
    # -------------------------Resolve job parameters-------------------------------
    params = [
        'JOB_NAME',
        'output_database',
        'source_database',
        'source_table',
        'output_table',
        'region',
        'partition_keys'
    ]

    args = getResolvedOptions(sys.argv, params)
    # ------------------------------------------------------------------------------

    # -------------------------Extract provided args--------------------------------
    output_database = args['output_database']
    source_database = args['source_database']
    source_table = args['source_table']
    output_table = args['output_table']
    region = args['region']
    partition_keys = []
    if 'partition_keys' in args and args['partition_keys'] != "":
        partition_keys = [x.strip() for x in args['partition_keys'].split(',')]
    # ------------------------------------------------------------------------------

    # -------------------------Initialize Glue Context-----------------------------
    # Create Glue Context, Job and Spark Session
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    glue_client = boto3.client('glue', region_name=region)
    output_path = get_output_path(glue_client, output_database, output_table)
    # ------------------------------------------------------------------------------

    # -------------------------Read Source Dynamic Frame----------------------------
    dyf = read_from_catalog(glue_context, source_database, source_table)
    dyf = DynamicFrame.fromDF(dyf.toDF().sample(0.7), glue_context, "sampled")
    # ------------------------------------------------------------------------------

    # -------------------------Write Dynamic Frame to target -----------------------
    write_versioned_parquet(glue_client, glue_context, dyf, output_path, partition_keys, output_database,
                            output_table)
    # ------------------------------------------------------------------------------

    # -------------------------------------Job Cleanup------------------------------
    job.commit()


# ------------------------------------------------------------------------------


if __name__ == "__main__":
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame

    main()
