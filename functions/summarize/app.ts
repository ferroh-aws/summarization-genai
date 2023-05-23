import { S3Event, Context } from 'aws-lambda';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { InvokeEndpointCommand, SageMakerRuntimeClient } from '@aws-sdk/client-sagemaker-runtime';
import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import * as natural from 'natural';

const MaxTokens: number = parseInt(process.env.MAX_TOKENS!);
const OutputBucket: string = process.env.OUTPUT_BUCKET!;
const OutputPrefix: string = process.env.OUTPUT_PREFIX!;
const ParameterName: string = process.env.PARAMETER_NAME!;

let s3Client: S3Client = new S3Client({});
let sageMakerClient: SageMakerRuntimeClient = new SageMakerRuntimeClient({});
let ssmClient: SSMClient = new SSMClient({});
let getName = (key: string): string => { return key.includes('/') ? key.substring(key.lastIndexOf('/') + 1) : key }
let endpointName: string|undefined = undefined;

/**
 * Lambda handler invoked each time an object is created in the transcripts bucket.
 *
 * @param event The event containing the records of the new objects.
 * @param _ The execution context of the lambda, this is not used.
 */
exports.lambdaHandler = async (event: S3Event, _: Context): Promise<void> => {
  if (endpointName == undefined) {
    const resp = await ssmClient.send(new GetParameterCommand({
      Name: ParameterName
    }));
    endpointName = resp.Parameter!.Value!;
  }
  console.log(`Processing ${event.Records.length} records.`);
  for (let record of event.Records) {
    console.log(`Processing file ${record.s3.object.key}`);
    let s3Response = await s3Client.send(new GetObjectCommand({
      Bucket: record.s3.bucket.name,
      Key: record.s3.object.key,
      VersionId: record.s3.object.versionId
    }));
    let chunks = prepareChunks(await s3Response.Body?.transformToString()!);
    let summary = await generateSummary(chunks);
    s3Response = await s3Client.send(new PutObjectCommand({
      Body: summary,
      Bucket: OutputBucket,
      Key: OutputPrefix + '/' + getName(record.s3.object.key)
    }));
    console.log('Summary uploaded');
  }
}

/**
 * Divides the entire text in chunks bellow the maximum tokens accepted by the model. In this function we use the
 * natural library to tokenize the text in sentences and try to keep coherence in the chunks.
 *
 * @param text The text to divide in chunks.
 */
let prepareChunks = (text: string): Array<string> => {
  let chunks = new Array<string>();
  let tokenizer = new natural.SentenceTokenizer();
  let sentences = tokenizer.tokenize(text);
  let chunk = '';
  let length = 0;
  for (let sentence of sentences) {
    let combineLength = length + sentence.length;
    if (combineLength <= MaxTokens) {
      length = combineLength;
      chunk += sentence + ' ';
    } else {
      chunks.push(chunk);
      chunk = sentence + ' ';
      length = chunk.length;
    }
  }
  chunks.push(chunk);
  return chunks;
}

/**
 * Generates the summary of the document with the previously generated chunks.
 *
 * @param chunks Chunks of the document not exceeding the maximum tokens.
 */
let generateSummary = async (chunks: Array<string>): Promise<string> => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  let summaries = new Array<string>();
  for (let chunk of chunks) {
    const data = {
      'inputs': chunk
    };
    const smResponse = await sageMakerClient.send(new InvokeEndpointCommand({
      Accept: 'application/json',
      Body: encoder.encode(JSON.stringify(data)),
      ContentType: 'application/json',
      EndpointName: endpointName
    }));
    const inference = JSON.parse(decoder.decode(smResponse.Body));
    summaries.push(inference[0]['generated_text']);
  }
  return summaries.join(' ');
}