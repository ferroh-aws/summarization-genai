import { Context, S3EventRecord, SQSBatchItemFailure, SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { GetObjectCommand, PutObjectCommand, PutObjectCommandOutput, S3Client } from '@aws-sdk/client-s3';
import { StartExecutionCommand, SFNClient } from '@aws-sdk/client-sfn';
import { GetParameterCommand, SSMClient} from '@aws-sdk/client-ssm';
import { SentenceTokenizerNew, WordPunctTokenizer } from 'natural';

const ChunkPrefix: string = process.env.CHUNK_PREFIX!;
const Expiration: number = parseInt(process.env.EXPIRATION!);
const MaxTokens: number = parseInt(process.env.MAX_TOKENS!);
const ParameterName: string = process.env.PARAMETER_NAME!;
const StateMachineArn: string = process.env.STATE_MACHINE_ARN!;
const sfnClient: SFNClient = new SFNClient({});
const s3Client: S3Client = new S3Client({});
const ssmClient: SSMClient = new SSMClient({});
let endpointName: string|undefined = undefined;

/**
 * Lambda handler used to split the transcription and start the state machine for the summarization of each chunk.
 *
 * @param event The SQS event.
 * @param _
 */
exports.lambdaHandler = async (event: SQSEvent, _: Context): Promise<SQSBatchResponse> => {
  if (endpointName == undefined) {
    const resp = await ssmClient.send(new GetParameterCommand({
      Name: ParameterName
    }));
    endpointName = resp.Parameter!.Value!;
  }
  const failures = new Array<SQSBatchItemFailure>();
  console.log(`Starting split process for ${event.Records.length} messages.`);
  for (const sqsRecord of event.Records) {
    console.log(sqsRecord.body);
    const body = JSON.parse(sqsRecord.body);
    if (body['Records']) {
      console.log(`Processing ${body['Records'].length} new objects.`)
      for (const record of body['Records'] as Array<S3EventRecord> ) {
        console.log(`Splitting ${record.s3.object.key}`);
        const getObjectRes = await s3Client.send(new GetObjectCommand({
          Bucket: record.s3.bucket.name,
          Key: record.s3.object.key,
          VersionId: record.s3.object.versionId
        }));
        const chunks = splitText(await getObjectRes.Body!.transformToString());
        const summaryName = getName(record.s3.object.key);
        const keys = await putChunks(record.s3.bucket.name, summaryName, chunks);
        const data = {
          'inputs': chunks[0]
        };
        const input: SummaryEvent = {
          bucket: record.s3.bucket.name,
          current: 0,
          data: JSON.stringify(data),
          endpointName: endpointName,
          keys: keys,
          maxChunk: chunks.length - 1,
          name: summaryName,
          summary: undefined,
          summaryKeys: new Array<string>()
        };
        const date = (new Date()).toISOString()
          .replace(/:/g, '-')
          .replace(/\..+/, '');
        const response = await sfnClient.send(new StartExecutionCommand({
          name: `summarize-${getName(record.s3.object.key)}-${date}`,
          input: JSON.stringify(input),
          stateMachineArn: StateMachineArn
        }));
        console.log(`Started execution: ${response.executionArn}`);
      }
    }
  }
  return {
    batchItemFailures: failures
  };
}

/**
 * Divides the entire text in chunks bellow the maximum tokens accepted by the model. In this function we use the
 * natural library to tokenize the text in sentences and try to keep coherence in the chunks.
 *
 * @param text The text to divide in chunks.
 */
const splitText = (text: string): Array<string> => {
  let chunks = new Array<string>();
  let sentenceTokenizer = new SentenceTokenizerNew();
  let wordTokenizer = new WordPunctTokenizer();
  let sentences = sentenceTokenizer.tokenize(text);
  let chunk = '';
  let length = 0;
  for (let sentence of sentences) {
    let combineLength = length + wordTokenizer.tokenize(sentence)!.length;
    if (combineLength < MaxTokens) {
      length = combineLength;
      chunk += sentence + ' ';
    } else {
      console.log(`Chunk tokens: ${wordTokenizer.tokenize(chunk)!.length}`)
      console.log(chunk);
      chunks.push(chunk);
      chunk = sentence + ' ';
      length = wordTokenizer.tokenize(chunk)!.length;
    }
  }
  chunks.push(chunk);
  console.log(`Total chunks: ${chunks.length}`);
  return chunks;
}

/**
 * Upload the chunks to S3 to be sent to Amazon SageMaker endpoint.
 *
 * @param bucket
 * @param name
 * @param chunks
 */
const putChunks = async (bucket: string, name: string, chunks: Array<string>): Promise<Array<string>> => {
  const promises = new Array<Promise<PutObjectCommandOutput>>();
  const keys = new Array<string>();
  const expires = new Date(new Date().getTime() + Expiration * 60000);
  let count = 0;
  for (const chunk of chunks) {
    const key = `${ChunkPrefix}${(++count).toLocaleString('en-US', {minimumIntegerDigits: 2})}-${name}`;
    promises.push(
      s3Client.send(new PutObjectCommand({
        Body: chunk,
        Bucket: bucket,
        Expires: expires,
        Key: key
      }))
    );
    keys.push(key);
  }
  // Wait for all chunks to be uploaded.
  await Promise.all(promises);
  return keys;
}

/**
 * Strips the prefix of the object.
 * @param key The key to process.
 */
let getName = (key: string): string => { return key.includes('/') ? key.substring(key.lastIndexOf('/') + 1) : key }

/**
 * Auxiliary interface used to facilitate readability.
 */
interface SummaryEvent {
  bucket: string,
  data: string,
  endpointName: string,
  keys: Array<string>,
  current: number,
  maxChunk: number,
  name: string,
  summary: string|undefined,
  summaryKeys: Array<string>
}