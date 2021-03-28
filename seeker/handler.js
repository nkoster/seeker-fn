'use strict'

const DEBUG = true
const DEVDELAY = 0
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

const { Client } = require('pg')
const client = new Client(config)
const pidKiller = new Client(config)

const LIMIT = process.env.SQLLIMIT || 51

const sqlSelectQuery = queryId => {
  return `
SELECT kafka_topic, kafka_offset, identifier_type, identifier_value
/*${queryId}*/
FROM dist_identifier_20210312
-- FROM identifier_20210311
WHERE ($1 = '' OR kafka_topic ilike $1)
AND   ($2 = '' OR identifier_type ilike $2)
AND   ($3 = '' OR identifier_value ilike $3)
AND   RIGHT('0000000000' || CAST(kafka_offset AS VARCHAR(10)), 10) like $4
ORDER BY kafka_offset DESC
LIMIT ${LIMIT}
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

module.exports = async (event, context) => {

  let data

  const body = event.body

  try {
    await client.connect()
    await pidKiller.connect()
  } catch(err) {
    DEBUG && console.log(err.message)
  }

  const queryId = body.queryId
  DEBUG && console.log('queryId:', queryId)

  const query = {
    name: queryId,
    text: sqlSelectQuery(queryId),
    values: [
      `%${body.search.queryKafkaTopic}%`,
      `%${body.search.queryIdentifierType}%`,
      `%${body.search.queryIdentifierValue}%`,
      `%${body.search.queryKafkaOffset}%`
    ]
  }

  try {
    await pidKiller.query(sqlKillQuery(queryId))
  } catch (err) {
    DEBUG && console.log(err.message)
  }
  try {
    data = await client.query(query)
    DEBUG && console.log('rows:', data.rows.length)
  } catch (err) {
    DEBUG && console.log(err.message)
  }

  return context
    .headers(
      {
        'Content-type': 'application/json',
        // 'Access-Control-Allow-Origin': 'https://ui.fhirstation.net/'
      }
    )
    .status(200)
    .succeed(JSON.stringify(data ? data.rows : []))
}
