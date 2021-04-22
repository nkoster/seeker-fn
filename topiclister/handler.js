'use strict'

module.exports = async (event, context) => {

  const DEBUG = true
  const DEV_DELAY = 1000

  const fs = require('fs')
  
  const config = {
    database: process.env.PGDATABASE || 'postgres',
    user: process.env.PGUSER || 'postgres',
    host: process.env.PGHOST || 'db.fhirstation.net',
    port: process.env.PGPORT || '5432',
    ssl: {
      rejectUnauthorized: false,
      ca: fs.readFileSync('/home/app/function/tls/root.crt').toString(),
      key: fs.readFileSync('/home/app/function/tls/client_postgres.key').toString(),
      cert: fs.readFileSync('/home/app/function/tls/client_postgres.crt').toString()
    }
  }
  
  const { Pool } = require('pg')
  const clientPool = new Pool(config)
  
  let data

  const query = {
    name: 'GimmeTheTopix',
    text: 'SELECT * FROM dist_kafka_topic'
  }

  const client = await clientPool.connect()

  data = await new Promise(async (resolve, reject) => {
    let result
    try {
      result = await client.query(query)
    } catch (err) {
      reject( { rows: [] } )
      console.log(err.message)
    } finally {
      resolve(result)
      client.release()
    }    
  })

  DEBUG && console.log('rows:', data.rows.length)

  if (DEV_DELAY > 0) {
    await new Promise(resolve => setTimeout(resolve, DEV_DELAY))
  }

  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(JSON.stringify(data ? data.rows : []))
}
