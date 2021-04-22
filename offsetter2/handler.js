'use strict'

module.exports = async (event, context) => {

  const kafka = require('kafka-node')
  const Consumer = kafka.Consumer
  const client = new kafka.KafkaClient({kafkaHost: 'pvdevkafka01:9092'})
  
  const {topic, offset, partition} = event.body

  console.log('AAP', topic, offset, partition)

  const consumer = new Consumer(
    client, [
      { topic, partition: parseInt(partition) }
    ], { autoCommit: false }
  )
  
  consumer.setOffset(topic, parseInt(partition), parseInt(offset))
  
  let data
  
  data = await new Promise((resolve, reject) => {
    try {
      consumer.on('message', m => {
        if (m.offset === parseInt(offset)) {
          resolve(m)
          client.close()
        }
      })
    } catch(err) {
      reject({})
      console.log(err.message)
    }  
  })
  
  consumer.on('error', m => {
    console.log(m.message)
  })
  
  console.log(data)

  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(JSON.stringify(data))
    
}
