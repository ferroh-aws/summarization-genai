import { StartTranscriptionJobCommand, TranscribeClient } from '@aws-sdk/client-transcribe';
import { Context, S3Event } from "aws-lambda";

const transcribeClient: TranscribeClient = new TranscribeClient({});

exports.handler = async (event: S3Event, _: Context): Promise<void> => {
  for (const record of event.Records) {
    try {
      const bucketName = record.s3.bucket.name;
      const objectKey = record.s3.object.key;
      console.log(`New file added to bucket: ${bucketName}`);
      console.log(`File key: ${objectKey}`);
      console.log(`Will execute a transcription job using role: ${process.env.ROLE_ARN}`);

      //generate a 8 character random string
      const randomString = Math.random().toString(36).substring(2, 10);
      const outputKey = 'transcribe-jobs/' + objectKey.substring(objectKey.lastIndexOf('/') + 1, objectKey.lastIndexOf('.'))+'-'+randomString+".json";
      const fileFormat = objectKey.substring(objectKey.lastIndexOf('.')+1);
      console.log('Input file format: '+fileFormat);
      //create an amazon transcribe job using the object key and bucket name
      const response = await transcribeClient.send(new StartTranscriptionJobCommand({
        TranscriptionJobName: objectKey.substring(objectKey.lastIndexOf('/') + 1, objectKey.lastIndexOf('.'))+'-'+randomString,
        LanguageCode: 'en-US',
        MediaFormat: fileFormat,
        Settings: {
          ShowSpeakerLabels: true,
          MaxSpeakerLabels: 10,
          ShowAlternatives: false,
        },
        Media: {
          MediaFileUri: `s3://${bucketName}/${objectKey}`
        },
        OutputBucketName: bucketName,
        OutputKey: outputKey,
        JobExecutionSettings: {
          AllowDeferredExecution: false,
          DataAccessRoleArn: process.env.ROLE_ARN
        }
      }));
      console.log(`Successfully started job ${response}`)
    } catch (error) {
      console.log(error);
    }
  }
}