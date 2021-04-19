'use strict'

module.exports = async (event, context) => {

  const DEBUG = true
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
  const pidKillerPool = new Pool(config)
  
  const LIMIT = process.env.SQLLIMIT || 51
  
  const sqlSelectQuery = queryId => {
    return `
  SELECT /*${queryId}*/
    * from func_identifier(in_identifier_value => $3,
    in_identifier_type => $2,
    in_kafka_topic => $1 ,
    in_kafka_offset => $4,
    in_kafka_partition => null)
  `
  }
  
  const sqlKillQuery = queryId => {
    return `
  WITH pids AS (
    /*notthisone*/
    SELECT pid
    FROM   pg_stat_activity
    WHERE  query LIKE '%/*${queryId}*/%'
    AND    query NOT LIKE '%/*notthisone*/%'
    AND    state='active'
  )
  SELECT pg_cancel_backend(pid) FROM pids;
  `
  }

  let data

  const body = event.body

  const {queryId} = body
  DEBUG && console.log('queryId:', queryId)

  const query = {
    name: queryId,
    text: sqlSelectQuery(queryId),
    values: [
      body.search.queryKafkaTopic,
      body.search.queryIdentifierType,
      body.search.queryIdentifierValue,
      parseInt(body.search.queryKafkaOffset) ? parseInt(body.search.queryKafkaOffset) : null
    ]
  }

  console.log('offset', typeof body.search.queryKafkaOffset)

  const client = await clientPool.connect()
  const pidKiller = await pidKillerPool.connect()

  try {
    await client.query(sqlKillQuery(queryId))
  } catch (err) {
    DEBUG && console.log(err.message)
  } finally {
    pidKiller.release()
  }

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

  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(JSON.stringify(data ? data.rows : []))
}
