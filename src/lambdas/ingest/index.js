/* eslint-disable import/prefer-default-export */
import got from 'got' // eslint-disable-line import/no-unresolved
import { createIndex } from '../../lib/database-client.js'
import { ingestItems, publishResultsToSns } from '../../lib/ingest.js'
import getObjectJson from '../../lib/s3-utils.js'
import logger from '../../lib/logger.js'

const isSqsEvent = (event) => 'Records' in event && event.Records[0]?.eventSource === 'aws:sqs'
const isS3Event = (event) => 'Records' in event && event.Records[0]?.eventSource === 'aws:s3'

const isSnsMessage = (record) => record.Type === 'Notification'

const stacItemFromSnsMessage = async (message) => {
  if ('href' in message) {
    const { protocol, hostname, pathname } = new URL(message.href)

    if (protocol === 's3:') {
      return await getObjectJson({
        bucket: hostname,
        key: pathname.replace(/^\//, '')
      })
    }

    if (protocol.startsWith('http')) {
      return await got.get(message.href, {
        resolveBodyOnly: true
      }).json()
    }

    throw new Error(`Unsupported source: ${message.href}`)
  }

  return message
}

const stacItemFromRecord = async (record) => {
  const recordBody = JSON.parse(record.body)

  return isSnsMessage(recordBody)
    ? await stacItemFromSnsMessage(JSON.parse(recordBody.Message))
    : recordBody
}

const stacItemsFromSqsEvent = async (event) => {
  const records = event.Records

  return await Promise.all(
    records.map((r) => stacItemFromRecord(r))
  )
}

const stacItemFromS3Record = async (record) => {
  const bucket = record.s3.bucket.name
  const key = record.s3.object.key
  return await getObjectJson({
    bucket,
    key
  })
}

const stacItemsFromS3Event = async (event) => {
  const records = event.Records

  return await Promise.all(
    records.map((r) => stacItemFromS3Record(r))
  )
}

export const handler = async (event, _context) => {
  logger.debug('Event: %j', event)

  if (event.create_indices) {
    await createIndex('collections')
    return
  }

  let stacItems
  if (isSqsEvent(event)) {
    stacItems = await stacItemsFromSqsEvent(event)
  } else if (isS3Event(event)) {
    stacItems = await stacItemsFromS3Event(event)
  } else {
    stacItems = [event]
  }

  try {
    logger.debug('Attempting to ingest %d items', stacItems.length)

    const results = await ingestItems(stacItems)

    const errorCount = results.filter((result) => result.error).length
    if (errorCount) {
      logger.debug('There were %d errors ingesting %d items', errorCount, stacItems.length)
    } else {
      logger.debug('Ingested %d items', results.length)
    }

    const postIngestTopicArn = process.env['POST_INGEST_TOPIC_ARN']

    if (postIngestTopicArn) {
      logger.debug('Publishing to post-ingest topic: %s', postIngestTopicArn)
      await publishResultsToSns(results, postIngestTopicArn)
    } else {
      logger.debug('Skipping post-ingest notification since no topic is configured')
    }

    if (errorCount) throw new Error('There was at least one error ingesting items.')
  } catch (error) {
    logger.error(error)
    throw (error)
  }
}
