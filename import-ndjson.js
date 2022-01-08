#!/usr/bin/env node

const argv = require('yargs')
  .options({
    'table': {
      alias: 't',
      describe: 'PostgreSQL table',
      demandOption: true
    }
  })
  .options({
    'srid': {
      alias: 's',
      describe: 'SRID',
      demandOption: true
    }
  })
  .help('help')
  .argv

const H = require('highland')
const pg = require('pg')

const BATCH_SIZE = 250

const client = new pg.Client({
  user: 'postgis',
  host: 'localhost',
  database: 'postgis',
  password: 'postgis',
  port: 5432
})

async function insert (client, tableName, srid, batch) {
  const firstRow = batch[0]
  const hasId = firstRow.id !== undefined

  let query

  if (hasId) {
    const value = (row) => `(
      ${row.id},
      '${JSON.stringify({...row, id: undefined, geometry: undefined}).replace(/\'/g, '\'\'')}'::json,
      ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('${JSON.stringify(row.geometry)}'), 4326), ${srid})
    )`

    query = `
      INSERT INTO ${client.escapeIdentifier(tableName)} (id, data, geometry)
      VALUES ${batch.map(value).join(',')}`
  } else {
    const value = (row) => `(
      '${JSON.stringify({...row, geometry: undefined}).replace(/\'/g, '\'\'')}'::json,
      ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('${JSON.stringify(row.geometry)}'), 4326), ${srid})
    )`

    query = `
      INSERT INTO ${tableName} (data, geometry)
      VALUES ${batch.map(value).join(',')}`
  }

  await client.query(query)
}

async function run (tableName, srid) {
  await client.connect()
  await client.query(`TRUNCATE ${tableName}`)

  let count = 0

  console.error(`Importing data into ${tableName}`)

  H(process.stdin)
    .split()
    .compact()
    .map(JSON.parse)
    .map((row) => {
      count++
      return row
    })
    .batch(BATCH_SIZE)
    .flatMap((batch) => H(insert(client, tableName, srid, batch)))
    .done(async () => {
      console.error(`Done importing ${count} rows!`)
      await client.end()
    })
}

run(argv.table, parseInt(argv.srid))
