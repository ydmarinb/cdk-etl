import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';


/// --------------------------------------------------
const bucketName = 'etl-glue-snowflake-bf-jota';
const localCsvFilePath = 'data_sources/';
const bucketKeyPrefixData = 'data_source/';
const localScriptPath = 'script_jobs/';
const scriptName = 'file_to_snowflake.py';
const bucketKeyPrefixScripts = 'script_jobs/';
const roleName = 'glue_role';
const secretValueFromEnv = process.env.snowflake_pwd; // env var
const glueJobName = 'bf-job';
/// --------------------------------------------------


class EtlStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Create an S3 bucket
    const dataSourceBucket = new s3.Bucket(this, 'etl-glue-snowflake', {
      bucketName: bucketName,
      removalPolicy: RemovalPolicy.DESTROY, // Only for example purposes, change in production
    });

    // Upload 'data.csv' to the 'data_source' folder in the bucket
    new BucketDeployment(this, 'data_csv_deployment', {
      sources: [Source.asset(localCsvFilePath)],
      destinationBucket: dataSourceBucket,
      destinationKeyPrefix: bucketKeyPrefixData,
    });

    // Upload a Python script to the same S3 bucket
    new BucketDeployment(this, 'python_script_deployment', {
      sources: [Source.asset(localScriptPath)],
      destinationBucket: dataSourceBucket,
      destinationKeyPrefix: bucketKeyPrefixScripts,
    });

    // Create a Glue role
    const glue_role = new iam.Role(this, 'GlueRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      roleName: roleName,
    });

    glue_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));

    // Allow the Glue job to access the S3 bucket
    dataSourceBucket.grantRead(glue_role);

    // Verifica si la variable de entorno está definida
    if (!secretValueFromEnv) {
      throw new Error('La variable de entorno snowflake_pwd no está definida.');
    }
    const glueSecret = new secretsmanager.Secret(this, 'MySecret', {
      secretName: 'secret_snowflake_pwd_cdk',
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ snowflake_pwd: secretValueFromEnv }),
        generateStringKey: 'password', // Esto genera una contraseña si SecretString no se proporciona
      },
    });

    // Allow the Glue role to access the secret
    glueSecret.grantRead(glue_role);

    // Create an AWS Glue job
    const glue_job = new glue.CfnJob(this, 'GlueJob', {
      name: glueJobName,
      role: glue_role.roleArn,
      command: {
        name: 'pythonshell',
        scriptLocation: `s3://${dataSourceBucket.bucketName}/${bucketKeyPrefixScripts}${scriptName}`,
        pythonVersion: '3.9'
      },
      defaultArguments: {
        //'--input-path': `s3://${dataSourceBucket.bucketName}/data_source/data.csv`,
        //'--output-path': 's3://ruta-de-salida',
        '--additional-python-modules': 'pydantic==1.10.13,snowflake-connector-python',
        // Reference to the secrets in Glue job
        //'--secrets': glueSecret.secretArn,
      },
      timeout: 5,
    });
  }
}

export { EtlStack };
