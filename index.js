/**
 * @author Jose Nidhin
 */
'use strict';

const Archiver = require('archiver'),
  AWS = require('aws-sdk'),
  { PassThrough } = require('stream'),
  Pino = require('pino'),

  CONTENT_TYPE_CSV = 'text/csv',
  CONTENT_TYPE_ZIP = 'application/zip',

  CSV_ROWS = 100,

  Logger = Pino(),

  S3 = new AWS.S3({
    apiVersion: '2006-03-01'
  });

function generateCSVData (rows) {
  return Array(rows)
  .fill(Math.random())
  .map((d, i) => d * (i + 1))
  .join(',\n');
}

function generateFiles (num) {
  return Array(num)
  .fill({})
  .map((d, i) => {
    return {
      name: `${i + 1}.csv`,
      data: generateCSVData(CSV_ROWS)
    };
  });
}

function s3Get ({ bucket, key }) {
  const functionName = 's3Get',
    params = {
      Bucket: bucket,
      Key: key
    };

  Logger.info({ functionName, params });

  return S3.getObject({ Bucket: bucket, Key: key })
  .createReadStream();
}

async function s3Put ({ bucket, key, contentType, body }) {
  const functionName = 's3Put',
    params = {
      Bucket: bucket,
      Key: key,
      ContentType: contentType,
      Body: body
    };

  Logger.info({ functionName, params });

  try {
    const response = await S3.putObject(params).promise();
    Logger.info({ response, functionName }, 'S3 putObject response');
  } catch (err) {
    Logger.warn({ err, functionName }, 'S3 putObject failed');
    throw new Error('S3 put failed');
  }
}

async function s3Upload ({ bucket, key, contentType, stream }) {
  const functionName = 's3Upload',
    params = {
      Bucket: bucket,
      Key: key,
      ContentType: contentType,
      Body: stream
    };

  Logger.info({
    functionName,
    params: {
      Bucket: params.Bucket,
      Body: '[IGNORED]',
      Key: params.Key,
      ContentType: params.ContentType
    }
  });

  try {
    const response = await S3.upload(params).promise();
    Logger.info({ response, functionName }, 'S3 upload response');
  } catch (err) {
    Logger.warn({ err, functionName }, 'S3 upload failed');
    throw new Error('S3 streamUpload failed');
  }
}

async function main () {
  const bucket = process.argv[2] || 'jose-test-bucket',
    files = generateFiles(2);

  Logger.info({ files });

  const promises = files.map((file) => {
    return s3Put({
      bucket,
      key: file.name,
      contentType: CONTENT_TYPE_CSV,
      body: file.data
    });
  });

  try {
    await Promise.allSettled(promises);
  } catch (err) {
    Logger.error({ err });
  }

  const archiveStream = Archiver('zip'),
    zipToS3 = new PassThrough();

  archiveStream.on('error', (err) => {
    Logger.error({ err });
  });

  archiveStream.pipe(zipToS3);

  const uploadPromise = s3Upload({
    bucket,
    key: 'demo.zip',
    contentType: CONTENT_TYPE_ZIP,
    stream: zipToS3
  });

  for (const file of files) {
    const fileStream = s3Get({
      bucket,
      key: file.name
    });

    archiveStream.append(fileStream, { name: file.name });
  }

  archiveStream.finalize();

  await uploadPromise;
}

main();
