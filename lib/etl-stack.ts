import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';

class EtlStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Create an S3 bucket
    const data_source_bucket = new s3.Bucket(this, 'etl-glue-snowflake', {
      bucketName: 'etl-glue-snowflake-bf',
      removalPolicy: RemovalPolicy.DESTROY, // Only for example purposes, change in production
    });

    // Upload 'data.csv' to the 'data_source' folder in the bucket
    const local_csv_file_path = 'data_sources/';
    new BucketDeployment(this, 'data_csv_deployment', {
      sources: [Source.asset(local_csv_file_path)],
      destinationBucket: data_source_bucket,
      destinationKeyPrefix: 'data_source/',
    });

    // Upload a Python script to the same S3 bucket
    const local_script_path = 'script_jobs/';
    new BucketDeployment(this, 'python_script_deployment', {
      sources: [Source.asset(local_script_path)],
      destinationBucket: data_source_bucket,
      destinationKeyPrefix: 'script_jobs/',
    });

    // Create a Glue role
    const glue_role = new iam.Role(this, 'GlueRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    glue_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));

    // Allow the Glue job to access the S3 bucket
    data_source_bucket.grantRead(glue_role);

    // Create a secret to store sensitive data for Glue
// Create a new Secret
  const glueSecret = new secretsmanager.Secret(this, 'MySecret', {
    secretName: 'secret-snowflake',
    // You can add key-value pairs that you want to store in the secret
    generateSecretString: {
      secretStringTemplate: JSON.stringify({ SecretString: '098903834890308409329' })
    },
  });

    // Allow the Glue role to access the secret
    glueSecret.grantRead(glue_role);

    // Create an AWS Glue job
    const glue_job = new glue.CfnJob(this, 'GlueJob', {
      name: 'bf-job',
      role: glue_role.roleArn,
      command: {
        name: 'pythonshell',
        scriptLocation: `s3://${data_source_bucket.bucketName}/script_jobs/file_to_snowflake.py`,
        pythonVersion: '3.9'
      },
      defaultArguments: {
        //'--input-path': `s3://${data_source_bucket.bucketName}/data_source/data.csv`,
        //'--output-path': 's3://ruta-de-salida',
        '--additional-python-modules': 'pydantic==1.10.13,snowflake-connector-python',
        // Reference to the secrets in Glue job
        //'--secrets': glueSecret.secretArn,
      },
    });
  }
}

export { EtlStack };
