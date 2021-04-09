'use strict'

const DEBUG = true

// const consumer = kafka.consumer({ groupId: 'offsetter' + Math.random().toString(20).substr(2) })

module.exports = async (event, context) => {

  const groupId = Math.random().toString(20).substr(2)
  const tmpFile = '/tmp/' + groupId

  const { Kafka } = require('kafkajs')
  const fs = require('fs')

  const kafka = new Kafka({
    clientId: 'offsetter',
    brokers: ['pvdevkafka01:9092']
  })

  const consumer = kafka.consumer({ groupId })

  const {topic, offset, partition} = event.body

  DEBUG && console.log('TOPIC OFFSET PARTITION', topic, offset, partition)

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

  let result = ''

  fs.open(tmpFile, 'w', err => {
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
        console.log('PARTITION:', batch.partition)
        let kafkaMessage = {}
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
          if (offset === message.offset) {
            DEBUG && console.log('MESSAGE:', kafkaMessage)
            fs.writeFileSync(tmpFile, JSON.stringify(kafkaMessage))
            consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }])
            consumer.disconnect()
            break
          }
        }
      }
    })
    consumer.seek({ topic, partition, offset })
  } catch(err) {
    console.log(err.message)
  }

  try {
    result = fs.readFileSync(tmpFile, 'utf8')
  } catch (err) {
    console.log(err.message)
  } finally {
    DEBUG && console.log('RESULT', result)
  }

  await new Promise(resolve => setTimeout(resolve, 500))
  await consumer.disconnect()
  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(JSON.parse(fs.readFileSync(tmpFile, 'utf8')))
}
