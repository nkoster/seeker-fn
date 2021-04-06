'use strict'

const DEBUG = true
const { Kafka } = require('kafkajs')
const fs = require('fs')

const kafka = new Kafka({
  clientId: 'offsetter',
  brokers: ['pvdevkafka01:9092']
})

const consumer = kafka.consumer({ groupId: 'offsetter' })

module.exports = async (event, context) => {

  const {topic, offset} = event.body

  DEBUG && console.log('TOPIC OFFSET', topic, offset)

  try {
    await consumer.disconnect()
  } catch (err) {
    console.log(err)
  }
  try {
    await consumer.connect()
  } catch (err) {
    console.log(err)
  }
  try {
    await consumer.subscribe({ topic, fromBeginning: true })      
  } catch(err) {
    console.log(err)
  }
  let kafkaMessage = {}
  let partition = 0

  let result = ''

  fs.open('/tmp/k', 'w', err => {
    if (err) throw err
  })

  try {
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch, isStale }) => {
        if (isStale()) {
          return
        }
        for (let message of batch.messages) {
          kafkaMessage = {
            topic: batch.topic,
            partition: batch.partition,
            highWatermark: batch.highWatermark,
            message: {
              offset: message.offset,
              key: message.key.toString(),
              value: message.value.toString(),
              headers: message.headers
            }
          }
          partition = batch.partition
          DEBUG && console.log('OFFSET', kafkaMessage)
          fs.writeFileSync('/tmp/k', JSON.stringify(kafkaMessage))
          console.log('TEST WRITE', JSON.parse(fs.readFileSync('/tmp/k', 'utf8')))
          consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }])
          consumer.disconnect()
          break
        }
      }
    })
    consumer.seek({ topic, partition, offset })
  } catch(err) {
    console.log(err.message)
  }

  try {
    result = fs.readFileSync('/tmp/k', 'utf8')
  } catch (err) {
    console.log(err.message)
  } finally {
    DEBUG && console.log('RESULT', result)
  }

  await new Promise(resolve => setTimeout(resolve, 1000))
  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(JSON.parse(fs.readFileSync('/tmp/k', 'utf8')))
}
