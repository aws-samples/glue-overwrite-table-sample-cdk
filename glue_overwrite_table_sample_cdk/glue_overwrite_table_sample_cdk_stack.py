# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import core as cdk


class GlueOverwriteTableSampleCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = f"glue-overwrite-table-sample-{self.account}"

        glue_service_role = iam.Role(
            self, id=f'glue_overwrite_table_sample_role',
            role_name=f'glue_overwrite_table_sample_role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
            ]
        )

        scripts_bucket = s3.Bucket(
            self, id='glue_overwrite_table_sample_scripts_bucket',
            bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            enforce_ssl=True,
            auto_delete_objects=True
        )

        glue_service_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=['s3:GetObject'],
            resources=["arn:aws:s3:::ookla-open-data/parquet/performance/*"]
        ))

        glue_service_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=['s3:GetObject', 's3:PutObject'],
            resources=[f'{scripts_bucket.bucket_arn}/*',
                       "arn:aws:s3:::ookla-open-data/parquet/performance/*"]
        ))

        s3_deployment.BucketDeployment(
            self, id=f'glue_overwrite_table_sample_scripts',
            sources=[s3_deployment.Source.asset('./lib')],
            destination_bucket=scripts_bucket,
            destination_key_prefix='scripts/',
            retain_on_delete=False
        )

        database_name = 'glue_overwrite_table_sample_db'
        database = glue.CfnDatabase(
            scope=self, id=database_name,
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                location_uri=f's3://{bucket_name}/glue_overwrite_table_sample_db',
                name=database_name
            )
        )

        source_table_name = 'overwrite_table_source_table_table'
        source_table = glue.CfnTable(
            scope=self, id=source_table_name,
            database_name=database_name,
            catalog_id=self.account,
            table_input=glue.CfnTable.TableInputProperty(
                name=source_table_name,
                table_type="EXTERNAL_TABLE",
                partition_keys=[glue.CfnTable.ColumnProperty(name="type", type="string"),
                                glue.CfnTable.ColumnProperty(name="year", type="string"),
                                glue.CfnTable.ColumnProperty(name="quarter", type="string")],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    location=f"s3://ookla-open-data/parquet/performance/",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        parameters={
                            "serialization.format": "1"
                        },
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                    parameters={
                        'classification': 'parquet',
                        'compressionType': 'none'
                    },
                    columns=[
                        glue.CfnTable.ColumnProperty(name='quadkey', type="string"),
                        glue.CfnTable.ColumnProperty(name='tile', type="string"),
                        glue.CfnTable.ColumnProperty(name='avg_d_kbps', type="bigint"),
                        glue.CfnTable.ColumnProperty(name='avg_u_kbps', type="bigint"),
                        glue.CfnTable.ColumnProperty(name='avg_lat_ms', type="bigint"),
                        glue.CfnTable.ColumnProperty(name='tests', type="bigint"),
                        glue.CfnTable.ColumnProperty(name='devices', type="bigint")
                    ]
                )
            )
        )
        source_table.add_depends_on(database)

        create_partitions(self, database, source_table, ["fixed", "mobile"], ["2019", "2020", "2021"],
                          ["1", "2", "3", "4"])

        glue.CfnJob(
            scope=self,
            id='glue_overwrite_table_sample_glue_job_cdk',
            name='glue_overwrite_table_sample_glue_job_cdk',
            description='This job overwrites the target table by replacing it with high availability',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{bucket_name}/scripts/overwrite_glue.py'
            ),
            role=glue_service_role.role_arn,
            default_arguments={
                '--output_database': database_name,
                '--source_database': database_name,
                '--source_table': source_table_name,
                '--output_table': 'glue_overwrite_table_sample_output_glue_job_table',
                '--region': self.region,
                '--partition_keys': 'type,year,quarter'
            },
            glue_version='3.0',
            number_of_workers=10,
            worker_type='G.1X'

        )


def create_partitions(self, database, source_table, types, years, quarters, ):
    for type in types:
        for year in years:
            for quarter in quarters:
                partition = glue.CfnPartition(
                    scope=self, id=f"{type}_{year}_{quarter}_partition",
                    catalog_id=self.account,
                    database_name=database.database_input.name,
                    table_name=source_table.table_input.name,
                    partition_input=glue.CfnPartition.PartitionInputProperty(
                        values=[type, year, quarter],
                        parameters={
                            'classification': 'parquet',
                            'compressionType': 'none'
                        },
                        storage_descriptor=glue.CfnPartition.StorageDescriptorProperty(
                            input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            location=f"s3://ookla-open-data/parquet/performance/type={type}/year={year}/quarter={quarter}",
                            output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            serde_info=glue.CfnPartition.SerdeInfoProperty(
                                parameters={
                                    "serialization.format": "1"
                                },
                                serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

                            )
                        )
                    )
                )
                partition.add_depends_on(source_table)
