#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import core

from glue_overwrite_table_sample_cdk.glue_overwrite_table_sample_cdk_stack import GlueOverwriteTableSampleCdkStack

app = core.App()

GlueOverwriteTableSampleCdkStack(app, "GlueOverwriteTableSampleCdkStack")

core.Tag.add(app, 'Creator', 'GlueOverwriteTableSampleCdk')

app.synth()
