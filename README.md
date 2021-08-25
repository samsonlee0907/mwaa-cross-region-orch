# Amazon Managed Workflow for Apache Airflow (MWAA) Orchestrating Cross-region Resources

This repository provides a sample DAG scripts to orchestrate resources in regions different from the region where the Managed Workflow for Apache Airflow (MWAA) service is hosted leveraging MWAA's Python operator with boto3 SDK.

The example uses ap-east-1 as a region where Amazon MWAA is not available yet, to orchestrate the provisioning of an EMR cluster.

With the Python Operator, you may leverage below line of code to configure the region that you'd want to orchestrate the resource in:

`client = boto3.client("aws_service_name", region_name='aws_region_name')`

As an example if I need to orchetrate resources with EMR in Hong Kong region (ap-east-1):

`client = boto3.client("emr", region_name='ap-east-1')`

You may visit the [Full Sample Code](https://github.com/samsonlee0907/mwaa-cross-region-orch/blob/main/cross_region_emr.py) here for the whole setup.

Here shows Amazon MWAA successfully created a cluster in ap-east-1 region:
![Log](/mwaa_log.png)
