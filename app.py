import aws_cdk as cdk
import boto3

from cdk_infrastructure import CDCStack

app = cdk.App()
environment = app.node.try_get_context("environment")
account = boto3.client("sts").get_caller_identity()["Account"]
response = (
    boto3.Session(region_name=environment["AWS_REGION"])
    .client("ec2")
    .describe_vpc_endpoint_services(
        ServiceNames=[f"com.amazonaws.{environment['AWS_REGION']}.dms"]
    )
)
dms_availability_zones = response["ServiceDetails"][0]["AvailabilityZones"]
environment["DMS_AVAILABILITY_ZONES"] = dms_availability_zones
response = (
    boto3.Session(region_name=environment["AWS_REGION"])
    .client("ec2")
    .describe_availability_zones()
)
all_availability_zones = [az["ZoneName"] for az in response["AvailabilityZones"]]
environment["ALL_AVAILABILITY_ZONES"] = all_availability_zones
stack = CDCStack(
    app,
    "CDCStack",
    env=cdk.Environment(account=account, region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
