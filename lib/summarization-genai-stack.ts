import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Architecture } from 'aws-cdk-lib/aws-lambda';
import { S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Bucket, BucketAccessControl, BucketEncryption, EventType } from 'aws-cdk-lib/aws-s3';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class SummarizationGenaiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const transcriptsBucket = new Bucket(this, 'transcripts-bucket', {
      accessControl: BucketAccessControl.PRIVATE,
      bucketName: 'transcripts-bucket-' + cdk.Aws.ACCOUNT_ID,
      encryption: BucketEncryption.S3_MANAGED
    });

    const endpointParam = StringParameter.fromStringParameterName(this, 'summarization-endpoint',
      'summarization_endpoint');

    const summarizationRole = new Role(this, 'summarization-role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
        'ssm': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'ssm:GetParameters'
              ],
              effect: Effect.ALLOW,
              resources: [
                endpointParam.parameterArn
              ]
            })
          ]
        }),
        's3': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                's3:GetObject',
                's3:PutObject'
              ],
              effect: Effect.ALLOW,
              resources: [
                transcriptsBucket.bucketArn + '/*'
              ]
            })
          ]
        }),
        'sagemaker': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'sagemaker:InvokeEndpoint'
              ],
              effect: Effect.ALLOW,
              resources: [
                'arn:aws:sagemaker:' + cdk.Aws.REGION + ':' + cdk.Aws.ACCOUNT_ID + ':endpoint/*'
              ]
            })
          ]
        })
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('AWSLambdaBasicExecutionRole')
      ]
    });

    const summarizationFunction = new NodejsFunction(this, 'summarization-function', {
      architecture: Architecture.X86_64,
      functionName: 'summarization-function',
      environment: {
        'MAX_TOKENS': '1022',
        'OUTPUT_BUCKET': transcriptsBucket.bucketName,
        'OUTPUT_PREFIX': 'summaries',
        'PARAMETER_NAME': endpointParam.parameterName
      },
      entry: 'functions/summarize/app.ts',
      events: [
        new S3EventSource(transcriptsBucket, {
          events: [EventType.OBJECT_CREATED],
          filters: [
            {
              prefix: 'transcript/'
            }
          ]
        })
      ],
      handler: 'lambdaHandler',
      role: summarizationRole
    });
  }
}
