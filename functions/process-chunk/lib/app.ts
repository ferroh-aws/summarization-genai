import { Context } from 'aws-lambda';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

const OutputPrefix: string = process.env.OUTPUT_PREFIX!;
const s3Client: S3Client = new S3Client({});

/**
 * Auxiliary interface to facilitate the readability.
 */
interface SummaryEvent {
  bucket: string,
  data: string,
  endpointName: string,
  keys: Array<string>,
  current: number,
  maxChunk: number,
  name: string,
  summary: any|undefined,
  summaryKeys: Array<string>
}

/**
 * Loads the next chunk to summarize and creates the payload to invoke the SageMaker endpoint.
 * @param event StepFunctions event.
 * @param _ Context not used.
 */
exports.lambdaHandler = async (event: SummaryEvent, _: Context): Promise<SummaryEvent> => {
  // Loading the next chunk.
  const next = event.current + 1;
  let data = event.data;
  if (next < event.maxChunk) {
    console.log(`Loading next chunk to summarize ${next}`);
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: event.bucket,
      Key: event.keys[next]
    }));
    const chunk = await response.Body!.transformToString();
    data = JSON.stringify({
      'inputs': chunk
    });
  }
  if (event.summary?.Body) {
    const body = JSON.parse(event.summary.Body);
    const key = `${OutputPrefix}${event.current.toLocaleString(
      'en-US', {minimumIntegerDigits: 2})}-sum-${event.name}`;
    event.summaryKeys.push(key);
    await s3Client.send(new PutObjectCommand({
      Body: body[0]['generated_text'],
      Bucket: event.bucket,
      Key: key
    }));
  }
  return {
    bucket: event.bucket,
    data: data,
    endpointName: event.endpointName,
    keys: event.keys,
    current: next,
    maxChunk: event.maxChunk,
    name: event.name,
    summary: undefined,
    summaryKeys: event.summaryKeys
  };
}