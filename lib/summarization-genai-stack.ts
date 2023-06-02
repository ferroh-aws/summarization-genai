import * as cdk from 'aws-cdk-lib';
import { RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Key } from 'aws-cdk-lib/aws-kms';
import { Architecture } from 'aws-cdk-lib/aws-lambda';
import { S3EventSource, SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Bucket, BucketAccessControl, BucketEncryption, EventType } from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Choice, Condition, CustomState, StateMachine } from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class SummarizationGenaiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const queue = new Queue(this, 'transcriptions-queue', {
      encryption: QueueEncryption.SQS_MANAGED,
      queueName: 'transcripts-queue'
    });

    const key = new Key(this, 'transcripts-bucket-key', {
      description: 'KMS key for S3 bucket encryption.'
    });

    const transcriptsBucket = new Bucket(this, 'transcripts-bucket', {
      accessControl: BucketAccessControl.PRIVATE,
      bucketName: 'transcripts-bucket-' + cdk.Aws.ACCOUNT_ID,
      encryption: BucketEncryption.KMS,
      encryptionKey: key,
      removalPolicy: RemovalPolicy.DESTROY
    });
    transcriptsBucket.addEventNotification(EventType.OBJECT_CREATED, new SqsDestination(queue), {
      prefix: 'transcripts/'
    });

    const endpointParam = StringParameter.fromStringParameterName(this, 'summarization-endpoint',
      'summarization_endpoint');

    const processChunkRole = new Role(this, 'process-chunk-role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
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
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });
    key.grantEncryptDecrypt(processChunkRole);

    const joinSummaryRole = new Role(this, 'join-summary-role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
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
        })
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });
    key.grantEncryptDecrypt(joinSummaryRole);

    const summarizationRole = new Role(this, 'summarization-role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
        'ssm': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'ssm:GetParameter'
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
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });

    const processChunkFunction = new NodejsFunction(this, 'process-chunk-function', {
      architecture: Architecture.X86_64,
      bundling: {
        forceDockerBundling: true,
        minify: true,
        sourceMap: true,
        sourcesContent: true,
        target: 'es2020'
      },
      description: 'This lambda loads the next chunk from S3 and if a summary is available is stored in S3.',
      functionName: 'process-chunk-function',
      entry: 'functions/process-chunk/lib/app.ts',
      environment: {
        'OUTPUT_PREFIX': 'summaries/'
      },
      handler: 'lambdaHandler',
      memorySize: 128,
      timeout: cdk.Duration.minutes(1),
      role: processChunkRole
    });

    const joinSummaryFunction = new NodejsFunction(this, 'join-summary-function', {
      architecture: Architecture.X86_64,
      bundling: {
        forceDockerBundling: true,
        minify: true,
        sourceMap: true,
        sourcesContent: true,
        target: 'es2020'
      },
      description: 'This lambda loads the chunk summaries and joins them.',
      functionName: 'join-summary-function',
      entry: 'functions/join-summaries/lib/app.ts',
      environment: {
        'OUTPUT_PREFIX': 'summaries/'
      },
      handler: 'lambdaHandler',
      memorySize: 128,
      timeout: cdk.Duration.minutes(1),
      role: joinSummaryRole
    });

    const summarizationFunction = new NodejsFunction(this, 'summarization-function', {
      architecture: Architecture.X86_64,
      bundling: {
        forceDockerBundling: true,
        minify: true,
        sourceMap: true,
        sourcesContent: true,
        target: 'es2020'
      },
      description: 'This lambda function reads the transcript, generate chunks and generate summaries for each chunk.',
      functionName: 'summarization-function',
      entry: 'functions/summarize/lib/app.ts',
      environment: {
        'MAX_TOKENS': '800',
        'OUTPUT_BUCKET': transcriptsBucket.bucketName,
        'OUTPUT_PREFIX': 'summaries',
        'PARAMETER_NAME': endpointParam.parameterName
      },
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
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      role: summarizationRole
    });

    const stepFunctionsRole = new Role(this, 'state-machine-role', {
      assumedBy: new ServicePrincipal('states.amazonaws.com'),
      inlinePolicies: {
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
        }),
        'lambda': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'lambda:InvokeFunction'
              ],
              effect: Effect.ALLOW,
              resources: [
                processChunkFunction.functionArn,
                joinSummaryFunction.functionArn
              ]
            })
          ]
        })
      }
    });

    const invokeEndpoint = new CustomState(this, 'invoke-sm-endpoint', {
      stateJson: {
        Type: 'Task',
        Resource: "arn:aws:states:::aws-sdk:sagemakerruntime:invokeEndpoint",
        Parameters: {
          "Accept": "application/json",
          "Body.$": "$.data",
          "ContentType": "application/json",
          "EndpointName.$": "$.endpointName"
        },
        ResultPath: "$.summary"
      }
    });

    const processChunk = new tasks.LambdaInvoke(this, 'invoke-process-chunk', {
      lambdaFunction: processChunkFunction,
      outputPath: '$.Payload'
    });

    const joinSummaries = new tasks.LambdaInvoke(this, 'invoke-join-summaries', {
      lambdaFunction: joinSummaryFunction
    });

    const definition = invokeEndpoint
      .next(processChunk)
      .next(new Choice(this, 'more-chunks?')
        .when(Condition.numberLessThanEqualsJsonPath('$.current', '$.maxChunk'), invokeEndpoint)
        .otherwise(joinSummaries)
      );

    const stateMachine = new StateMachine(this, 'summarization-state-machine', {
      definition: definition,
      timeout: cdk.Duration.minutes(30),
      role: stepFunctionsRole
    });

    const splitRole = new Role(this, 'split-role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
        'ssm': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'ssm:GetParameter'
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
        'sfn': new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                'states:StartExecution'
              ],
              effect: Effect.ALLOW,
              resources: [
                stateMachine.stateMachineArn
              ]
            })
          ]
        })
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });
    key.grantEncryptDecrypt(splitRole);

    const splitFunction = new NodejsFunction(this, 'split-transcript-function', {
      architecture: Architecture.X86_64,
      bundling: {
        forceDockerBundling: true,
        minify: true,
        sourceMap: true,
        sourcesContent: true,
        target: 'es2020'
      },
      description: 'This lambda function splits the transcript into chunks using the maximum tokens of the model.',
      functionName: 'split-transcript-function',
      entry: 'functions/split-transcript/lib/app.ts',
      environment: {
        'CHUNK_PREFIX': 'chunks/',
        'EXPIRATION': '120',
        'MAX_TOKENS': '800',
        'PARAMETER_NAME': endpointParam.parameterName,
        'STATE_MACHINE_ARN': stateMachine.stateMachineArn
      },
      events: [
        new SqsEventSource(queue, {
          batchSize: 1
        })
      ],
      handler: 'lambdaHandler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(1),
      role: splitRole
    });
  }
}
