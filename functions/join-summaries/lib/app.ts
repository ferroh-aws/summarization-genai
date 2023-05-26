import { Context } from 'aws-lambda';
import { GetObjectCommand, GetObjectCommandOutput, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

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
  summary: string|undefined,
  summaryKeys: Array<string>
}

/**
 * Lambda handler used to join all the summaries generated of the chunks.
 *
 * @param event The StepFunctions data.
 * @param _ Execution context not used.
 */
exports.lambdaHandler = async (event: SummaryEvent, _: Context): Promise<SummaryEvent> => {
  console.log(`Joining summary for ${event.name}`);
  const responses = Array<Promise<GetObjectCommandOutput>>();
  for (const key of event.summaryKeys) {
    responses.push(
      s3Client.send(new GetObjectCommand({
        Bucket: event.bucket,
        Key: key
      }))
    );
  }
  const objects = await Promise.all(responses);
  let summary = '';
  for (const obj of objects) {
    summary += await obj.Body!.transformToString() + ' ';
  }
  await s3Client.send(new PutObjectCommand({
    Body: summary,
    Bucket: event.bucket,
    Key: OutputPrefix + event.name
  }));
  return event;
}