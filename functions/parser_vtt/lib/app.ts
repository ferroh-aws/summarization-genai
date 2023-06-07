import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Context, S3Event } from "aws-lambda";

const s3Client: S3Client = new S3Client({});

exports.handler = async (event: S3Event, _: Context): Promise<void> => {
  for (const record of event.Records) {
    const inputBucket = record.s3.bucket.name;
    const inputKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

    const getObjectParams = { Bucket: inputBucket, Key: inputKey };
    const command = new GetObjectCommand(getObjectParams);
    const inputData = await s3Client.send(command);

    const str = await inputData.Body!.transformToString();

    const outputData = formatData(JSON.parse(str));
    var outputKey = inputKey;
    outputKey = 'transcripts/' + outputKey.substring(outputKey.lastIndexOf('/') + 1, outputKey.lastIndexOf('.')) + '.vtt';

    const putObjectCommand = new PutObjectCommand({ Bucket: inputBucket, Key: outputKey, Body: outputData });
    try {
      const response = await s3Client.send(putObjectCommand);
      console.log(response);
    } catch (err) {
      console.error(err);
    }
  }
}

function formatData(data: any) {
  const formatted = data.results.speaker_labels.segments.map((segment: any, i: number) => {
    const speaker = segment.speaker_label;
    const start_time = parseFloat(segment.start_time).toFixed(3).padStart(9, '0');
    const end_time = parseFloat(segment.end_time).toFixed(3).padStart(9, '0');
    var str_start_time = ''+start_time;
    var str_end_time = ''+end_time;

    str_start_time = '0'+str_start_time.substring(0,1)+":"+str_start_time.substring(1,3)+":"+str_start_time.substring(3);
    str_end_time = '0'+str_end_time.substring(0,1)+":"+str_end_time.substring(1,3)+":"+str_end_time.substring(3);

    //@ts-ignore
    const words = data.results.items.filter(item =>
      'start_time' in item && 'end_time' in item
      && parseFloat(item.start_time) >= parseFloat(segment.start_time)
      && parseFloat(item.end_time) <= parseFloat(segment.end_time)
      && item.speaker_label === speaker
      //@ts-ignore
    ).flatMap(item => item.alternatives.map(alt => alt.content));

    return `${speaker}: ${words.join(' ')}\n`;
  });

  return formatted.join('\n');
}