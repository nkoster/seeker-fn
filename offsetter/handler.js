'use strict'

const DEBUG = true

module.exports = async (event, context) => {

  const groupId = Math.random().toString(20).substr(2)

  const { Kafka } = require('kafkajs')

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

  const msg = await new Promise( async (resolve, reject) => {
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
              resolve(kafkaMessage)
              consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }])
              consumer.disconnect()
              break
            }
          }
        }
      })
      consumer.seek({ topic, partition, offset })
    } catch(err) {
      reject( {})
      console.log(err.message)
    }
  })

  await consumer.disconnect()

  console.log('MSG', msg)
  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(msg)
}
