import json

from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    RemovalPolicy,
    SecretValue,
    Stack,
    aws_dms as dms,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_rds as rds,
    aws_redshift as redshift,
    aws_s3 as s3,
    triggers,
)
from constructs import Construct


class RedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        vpc: ec2.Vpc,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.redshift_full_commands_full_access_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftAllCommandsFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        redshift_cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "RedshiftClusterSubnetGroup",
            subnet_ids=vpc.select_subnets(  # Redshift can exist within only 1 AZs
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ).subnet_ids,
            description="Redshift Cluster Subnet Group",
        )
        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            cluster_subnet_group_name=redshift_cluster_subnet_group.ref,  # needed or will use default VPC
            vpc_security_group_ids=[security_group.security_group_id],
            publicly_accessible=False,
        )


class RDSService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        vpc: ec2.Vpc,
        vpc_subnets: ec2.SubnetSelection,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        rds_subnet_group = rds.SubnetGroup(
            self,
            "RdsSubnetGroup",
            vpc=vpc,
            vpc_subnets=vpc_subnets,  # requires at least 2 AZs
            description="RDS Subnet Group",
        )
        self.rds_instance = rds.DatabaseInstance(
            self,
            "RDSForCDCToRedshift",
            engine=rds.DatabaseInstanceEngine.mysql(
                version=rds.MysqlEngineVersion.VER_8_0_28
            ),
            instance_type=ec2.InstanceType(
                "t3.micro"
            ),  # for demo purposes; otherwise defaults to m5.large
            credentials=rds.Credentials.from_username(
                username=environment["RDS_USER"],
                password=SecretValue.unsafe_plain_text(environment["RDS_PASSWORD"]),
            ),
            database_name=environment["RDS_DATABASE_NAME"],
            port=environment["RDS_PORT"],
            vpc=vpc,
            subnet_group=rds_subnet_group,
            security_groups=[security_group],
            parameters={  # needed for DMS replication task to run successfully
                "binlog_format": "ROW",
                "binlog_row_image": "full",
                "binlog_checksum": "NONE",
            },
            publicly_accessible=False,
            removal_policy=RemovalPolicy.DESTROY,
            delete_automated_backups=True,
        )

        self.configure_rds_lambda = _lambda.Function(  # will be used once in Trigger defined below
            self,  # purpose is to set MySQL binlog retention hours to 24
            "ConfigureRDSLambda",  # and create `RDS_TABLE_NAME` in the database
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/configure_rds_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py txns.csv /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={
                "CSV_FILENAME": environment["CSV_FILENAME"],
                "RDS_USER": environment["RDS_USER"],
                "RDS_PASSWORD": environment["RDS_PASSWORD"],
                "RDS_DATABASE_NAME": environment["RDS_DATABASE_NAME"],
                "RDS_TABLE_NAME": environment["RDS_TABLE_NAME"],
            },
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )
        self.load_data_to_rds_lambda = _lambda.Function(
            self,
            "LoadDataToRDSLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_data_to_rds_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py txns.csv /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={
                "CSV_FILENAME": environment["CSV_FILENAME"],
                "RDS_USER": environment["RDS_USER"],
                "RDS_PASSWORD": environment["RDS_PASSWORD"],
                "RDS_DATABASE_NAME": environment["RDS_DATABASE_NAME"],
                "RDS_TABLE_NAME": environment["RDS_TABLE_NAME"],
            },
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )

        # connect the AWS resources
        self.trigger_configure_rds_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRDSLambda",
            handler=self.configure_rds_lambda,  # this is underlying Lambda
            execute_after=[self.rds_instance],  # runs once after RDS creation
            execute_before=[  # before data is loaded to RDS
                self.load_data_to_rds_lambda
            ],
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_rds_lambda.timeout,
        )
        self.configure_rds_lambda.add_environment(
            key="RDS_HOST", value=self.rds_instance.db_instance_endpoint_address
        )
        self.load_data_to_rds_lambda.add_environment(
            key="RDS_HOST", value=self.rds_instance.db_instance_endpoint_address
        )


class CDCFromRDSToRedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        rds_endpoint_address: str,
        redshift_endpoint_address: str,
        vpc: ec2.Vpc,
        vpc_subnets: ec2.SubnetSelection,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.dms_rds_source_endpoint = dms.CfnEndpoint(
            self,
            "RDSSourceEndpoint",
            endpoint_type="source",
            engine_name="mysql",
            server_name=rds_endpoint_address,
            port=environment["RDS_PORT"],
            username=environment["RDS_USER"],
            password=environment["RDS_PASSWORD"],
        )
        self.dms_redshift_target_endpoint = dms.CfnEndpoint(
            self,
            "RedshiftTargetEndpoint",
            endpoint_type="target",
            engine_name="redshift",
            database_name=environment["REDSHIFT_DATABASE_NAME"],
            server_name=redshift_endpoint_address,
            port=environment["REDSHIFT_PORT"],
            username=environment["REDSHIFT_USER"],
            password=environment["REDSHIFT_PASSWORD"],
        )
        dms_subnet_group = dms.CfnReplicationSubnetGroup(
            self,
            "DmsSubnetGroup",
            subnet_ids=vpc.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ).subnet_ids,
            replication_subnet_group_description="DMS Subnet Group",
        )
        self.dms_replication_instance = dms.CfnReplicationInstance(
            self,
            "DMSReplicationInstance",
            replication_instance_class="dms.t3.micro",  # for demo purposes
            replication_subnet_group_identifier=dms_subnet_group.ref,  # needed or will use default VPC
            vpc_security_group_ids=[security_group.security_group_id],
            publicly_accessible=False,
        )
        self.dms_replication_task = dms.CfnReplicationTask(
            self,
            "DMSReplicationTask",
            migration_type="full-load-and-cdc",
            replication_instance_arn=self.dms_replication_instance.ref,  # appears that
            source_endpoint_arn=self.dms_rds_source_endpoint.ref,  # `ref` means
            target_endpoint_arn=self.dms_redshift_target_endpoint.ref,  # arn
            table_mappings=json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "%",
                                "table-name": environment["RDS_TABLE_NAME"],
                            },
                            "rule-action": "include",
                            "filters": [],
                        }
                    ]
                }
            ),
            replication_task_settings=json.dumps({"Logging": {"EnableLogging": True}}),
        )

        env_vars = {
            "PRINT_RDS_AND_REDSHIFT_NUM_ROWS": json.dumps(
                environment["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"]
            )
        }
        if environment["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"]:
            env_vars.update(
                {
                    "RDS_HOST": rds_endpoint_address,
                    "RDS_USER": environment["RDS_USER"],
                    "RDS_PASSWORD": environment["RDS_PASSWORD"],
                    "RDS_DATABASE_NAME": environment["RDS_DATABASE_NAME"],
                    "RDS_TABLE_NAME": environment["RDS_TABLE_NAME"],
                    "REDSHIFT_ENDPOINT_ADDRESS": redshift_endpoint_address,
                    "REDSHIFT_USER": environment["REDSHIFT_USER"],
                    "REDSHIFT_PASSWORD": environment["REDSHIFT_PASSWORD"],
                    "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                }
            )
        self.start_dms_replication_task_lambda = _lambda.Function(
            self,
            "StartDMSReplicationTaskLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/start_dms_replication_task_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment=env_vars,
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )
        self.start_dms_replication_task_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dms:StartReplicationTask", "dms:DescribeReplicationTasks"],
                resources=["*"],
            )
        )

        # connect the AWS resources
        self.start_dms_replication_task_lambda.add_environment(
            key="DMS_REPLICATION_TASK_ARN",
            value=self.dms_replication_task.ref,  # appears `ref` means arn
        )
        self.dms_endpoint = vpc.add_interface_endpoint(  # VPC endpoint needed
            "DmsEndpoint",  # by start_dms_replication_task_lambda
            service=ec2.InterfaceVpcEndpointAwsService.DATABASE_MIGRATION_SERVICE,
            subnets=vpc_subnets,
            security_groups=[security_group],
            # open=True,  ### idk what this does
        )


class DynamoDBService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        vpc: ec2.Vpc,
        vpc_subnets: ec2.SubnetSelection,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBTableForCDCToRedshift",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            # CDK wil not automatically deleted DynamoDB during `cdk destroy`
            # (as DynamoDB is a stateful resource) unless explicitly specified by the following line
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.s3_bucket_for_cdc_from_dynamodb_to_redshift = s3.Bucket(
            self,
            "DynamoDBStreamToRedshiftS3Bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,  # if versioning disabled, then expired files are deleted
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire_files_with_certain_prefix_after_1_day",
                    expiration=Duration.days(1),
                    prefix=f"{environment['PROCESSED_DYNAMODB_STREAM_FOLDER']}/",
                ),
            ],
        )

        self.load_data_to_dynamodb_lambda = _lambda.Function(
            self,
            "LoadDataToDynamoDBLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_data_to_dynamodb_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={"JSON_FILENAME": environment["JSON_FILENAME"]},
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )
        self.write_dynamodb_stream_to_s3_lambda = _lambda.Function(
            self,
            "WriteDynamoDBStreamToS3Lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/write_dynamodb_stream_to_s3_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
                "UNPROCESSED_DYNAMODB_STREAM_FOLDER": environment[
                    "UNPROCESSED_DYNAMODB_STREAM_FOLDER"
                ],
            },
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )

        # connect the AWS resources
        self.load_data_to_dynamodb_lambda.add_environment(
            key="DYNAMODB_TABLE_NAME", value=self.dynamodb_table.table_name
        )
        self.dynamodb_table.grant_write_data(self.load_data_to_dynamodb_lambda)
        self.write_dynamodb_stream_to_s3_lambda.add_environment(
            key="S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.s3_bucket_for_cdc_from_dynamodb_to_redshift.bucket_name,
        )
        self.write_dynamodb_stream_to_s3_lambda.add_event_source(
            event_sources.DynamoEventSource(
                self.dynamodb_table,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=100,  # hard coded
                max_batching_window=Duration.seconds(5),  # hard coded
                # filters=[{"event_name": _lambda.FilterRule.is_equal("INSERT")}]
            )
        )
        self.s3_bucket_for_cdc_from_dynamodb_to_redshift.grant_write(
            self.write_dynamodb_stream_to_s3_lambda
        )
        self.dynamodb_endpoint = vpc.add_gateway_endpoint(  # VPC endpoint needed
            "DynamodbEndpoint",  # by load_data_to_dynamodb_lambda
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
            subnets=[vpc_subnets],
        )


class CDCFromDynamoDBToRedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        s3_bucket_for_cdc_from_dynamodb_to_redshift: s3.Bucket,
        redshift_endpoint_address: str,
        redshift_role_arn: str,
        vpc: ec2.Vpc,
        vpc_subnets: ec2.SubnetSelection,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.configure_redshift_for_dynamodb_cdc_lambda = _lambda.Function(  # will be used once in Trigger defined below
            self,  # create the schema and table in Redshift for DynamoDB CDC
            "ConfigureRedshiftForDynamodbCDCLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/configure_redshift_for_dynamodb_cdc_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(10),  # may take some time
            memory_size=128,  # in MB
            environment={
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_PASSWORD": environment["REDSHIFT_PASSWORD"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC": environment[
                    "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC"
                ],
                "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC": environment[
                    "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC"
                ],
            },
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda = _lambda.Function(
            self,
            "LoadS3FilesFromDynamoDBStreamToRedshiftLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_s3_files_from_dynamodb_stream_to_redshift_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(20),  # may take some time if many files
            memory_size=128,  # in MB
            environment={
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_PASSWORD": environment["REDSHIFT_PASSWORD"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC": environment[
                    "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC"
                ],
                "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC": environment[
                    "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC"
                ],
                "AWSREGION": environment[
                    "AWS_REGION"
                ],  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "UNPROCESSED_DYNAMODB_STREAM_FOLDER": environment[
                    "UNPROCESSED_DYNAMODB_STREAM_FOLDER"
                ],
                "PROCESSED_DYNAMODB_STREAM_FOLDER": environment[
                    "PROCESSED_DYNAMODB_STREAM_FOLDER"
                ],
            },
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=[security_group],
        )

        # connect the AWS resources
        self.trigger_configure_redshift_for_dynamodb_cdc_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRedshiftForDynamodbCDCLambda",
            handler=self.configure_redshift_for_dynamodb_cdc_lambda,  # this is underlying Lambda
            # runs once after Redshift cluster created and before data loaded into Redshift
            execute_before=[self.load_s3_files_from_dynamodb_stream_to_redshift_lambda],
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_redshift_for_dynamodb_cdc_lambda.timeout,
        )
        self.configure_redshift_for_dynamodb_cdc_lambda.add_environment(
            key="REDSHIFT_ENDPOINT_ADDRESS", value=redshift_endpoint_address
        )
        lambda_environment_variables = {
            "S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT": s3_bucket_for_cdc_from_dynamodb_to_redshift.bucket_name,
            "REDSHIFT_ENDPOINT_ADDRESS": redshift_endpoint_address,
            "REDSHIFT_ROLE_ARN": redshift_role_arn,
        }
        for key, value in lambda_environment_variables.items():
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
                key=key, value=value
            )
        s3_bucket_for_cdc_from_dynamodb_to_redshift.grant_read_write(
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda
        )
        self.s3_endpoint = vpc.add_gateway_endpoint(  # VPC endpoint needed
            "S3Endpoint",  # by load_s3_files_from_dynamodb_stream_to_redshift_lambda
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[vpc_subnets],
        )


class CDCStack(Stack):
    @property  ### delete later
    def availability_zones(self):
        return self.all_availability_zones

    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.all_availability_zones = environment[  ### delete later
            "ALL_AVAILABILITY_ZONES"
        ]
        self.vpc = ec2.Vpc(
            self,
            "VPC",
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private-Subnet",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
            availability_zones=environment["DMS_AVAILABILITY_ZONES"][
                :2  # (RDS) DB subnet group needs at least 2 AZs
            ],
        )
        self.security_group_for_rds_redshift_dms = ec2.SecurityGroup(
            self,  # actually the "default" security group is sufficient
            "SecurityGroupForRDSRedshiftDMS",
            vpc=self.vpc,
            allow_all_outbound=True,
        )
        self.security_group_for_rds_redshift_dms.add_ingress_rule(  # for RDS + DMS
            peer=self.security_group_for_rds_redshift_dms,
            connection=ec2.Port.tcp(environment["RDS_PORT"]),
        )
        self.security_group_for_rds_redshift_dms.add_ingress_rule(  # for Redshift + DMS
            peer=self.security_group_for_rds_redshift_dms,
            connection=ec2.Port.tcp(environment["REDSHIFT_PORT"]),
        )
        self.security_group_for_rds_redshift_dms.add_ingress_rule(  # for Redshift + DMS
            peer=self.security_group_for_rds_redshift_dms,
            connection=ec2.Port.tcp(443),  # HTTPS for DMS endpoint for boto3
        )

        self.redshift_service = RedshiftService(
            self,
            "RedshiftService",
            environment=environment,
            vpc=self.vpc,
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.rds_service = RDSService(
            self,
            "RDSService",
            environment=environment,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.cdc_from_rds_to_redshift_service = CDCFromRDSToRedshiftService(
            self,
            "CDCFromRDSToRedshiftService",
            environment=environment,
            rds_endpoint_address=self.rds_service.rds_instance.db_instance_endpoint_address,
            redshift_endpoint_address=self.redshift_service.redshift_cluster.attr_endpoint_address,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.dynamodb_service = DynamoDBService(
            self,
            "DynamoDBService",
            environment=environment,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.cdc_from_dynamodb_to_redshift_service = CDCFromDynamoDBToRedshiftService(
            self,
            "CDCFromDynamoDBToRedshiftService",
            environment=environment,
            s3_bucket_for_cdc_from_dynamodb_to_redshift=self.dynamodb_service.s3_bucket_for_cdc_from_dynamodb_to_redshift,
            redshift_endpoint_address=self.redshift_service.redshift_cluster.attr_endpoint_address,
            redshift_role_arn=self.redshift_service.redshift_full_commands_full_access_role.role_arn,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_group=self.security_group_for_rds_redshift_dms,
        )

        # schedule Lambdas to run
        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEvery5Minutes",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(5)),
        )
        lambda_functions = [
            self.rds_service.load_data_to_rds_lambda,
            self.cdc_from_rds_to_redshift_service.start_dms_replication_task_lambda,
            self.dynamodb_service.load_data_to_dynamodb_lambda,
            self.cdc_from_dynamodb_to_redshift_service.load_s3_files_from_dynamodb_stream_to_redshift_lambda,
        ]
        for lambda_function in lambda_functions:
            self.scheduled_eventbridge_event.add_target(
                target=events_targets.LambdaFunction(
                    handler=lambda_function,
                    retry_attempts=3,
                    ### then put in DLQ
                ),
            )

        # write Cloudformation Outputs
        self.output_redshift_endpoint_address = CfnOutput(
            self,
            "RedshiftEndpointAddress",  # Output omits underscores and hyphens
            value=self.redshift_service.redshift_cluster.attr_endpoint_address,
        )
        self.output_rds_endpoint_address = CfnOutput(
            self,
            "RdsEndpointAddress",  # Output omits underscores and hyphens
            value=self.rds_service.rds_instance.db_instance_endpoint_address,
        )
        self.output_dms_vpc_endpoint_id = CfnOutput(
            self,
            "DmsVpcEndpointId",  # Output omits underscores and hyphens
            value=self.cdc_from_rds_to_redshift_service.dms_endpoint.vpc_endpoint_id,
        )
        self.output_dynamodb_table_name = CfnOutput(
            self,
            "DynamodbTableName",  # Output omits underscores and hyphens
            value=self.dynamodb_service.dynamodb_table.table_name,
        )
        self.output_s3_bucket_for_dynamodb_stream_to_redshift = CfnOutput(
            self,
            "S3BucketForDynamodbStreamToRedshift",  # Output omits underscores and hyphens
            value=self.dynamodb_service.s3_bucket_for_cdc_from_dynamodb_to_redshift.bucket_name,
        )
        self.output_dynamodb_vpc_endpoint_id = CfnOutput(
            self,
            "DynamodbVpcEndpointId",  # Output omits underscores and hyphens
            value=self.dynamodb_service.dynamodb_endpoint.vpc_endpoint_id,
        )
        self.output_s3_vpc_endpoint_id = CfnOutput(
            self,
            "S3VpcEndpointId",  # Output omits underscores and hyphens
            value=self.cdc_from_dynamodb_to_redshift_service.s3_endpoint.vpc_endpoint_id,
        )
