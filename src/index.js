'use strict'

import { Pool } from 'pg'
import escape from 'pg-escape'
import { map, uniq, difference } from 'lodash'

let masterPool, slavePool

export const handler = async (event) => {
  const options = {
    // the clone option sets if Kirby will leave the rows in the master DB
    // if false, the rows will be deleted from master after the copy
    clone: true,
    // the safe option sets if Kirby will fail if some of uuid's exist in the slave table
    safe: true,
    table: '',
    uuids: []
  }

  let masterClient
  let slaveClient

  const suck = async (event) => {
    const body = JSON.parse(event.body)
    options.table = body.table
    options.uuids = uniq(body.uuids.split(/,/))

    if (body.clone !== undefined) { options.clone = body.clone }

    if (body.safe !== undefined) { options.safe = body.safe }

    await connectToDBs(event)

    await checkTables()

    await checkRows()

    await moveRows()

    await closeDBConnections()
  }

  const connectToDBs = async (event) => {
    masterPool = new Pool({
      user: process.env.MASTER_PG_USER,
      host: process.env.MASTER_PG_ADDR,
      database: process.env.MASTER_PG_DB,
      password: process.env.MASTER_PG_PASS,
      port: process.env.MASTER_PG_PORT
    })
    slavePool = new Pool({
      user: process.env.SLAVE_PG_USER,
      host: process.env.SLAVE_PG_ADDR,
      database: process.env.SLAVE_PG_DB,
      password: process.env.SLAVE_PG_PASS,
      port: process.env.SLAVE_PG_PORT
    })

    masterClient = await masterPool.connect()
    await masterClient.query('SELECT NOW()')

    slaveClient = await slavePool.connect()
    await slaveClient.query('SELECT NOW()')
  }

  // checking that table exists both places
  const checkTables = async () => {
    const query = 'SELECT 1 ' +
                  'FROM information_schema.tables ' +
                  "WHERE table_schema='public' " +
                  "AND table_type='BASE TABLE'" +
                  'AND table_name = %L;'

    const checks = map([masterClient, slaveClient], async (client) => {
      const escaped = escape(query, options.table)
      const res = await client.query(escaped)
      if (res.rows.length === 0) {
        throw new Error(client.database + ' missing table: ' + options.table)
      }
      return res
    })

    await Promise.all(checks)
  }

  // checking that the rows exist in master db
  const checkRows = async () => {
    const expectedCount = options.uuids.length

    let query = escape('SELECT COUNT(*) FROM %I WHERE "id" in %L;', options.table, options.uuids)

    const masterRes = await masterClient.query(query)
    const diff = expectedCount - parseInt(masterRes.rows[0].count, 10)
    if (diff !== 0) {
      throw new Error(diff + ' uuids could not be found.')
    }

    // checking that the rows do not exist in slave db
    query = escape('SELECT id FROM %I WHERE "id" in %L;', options.table, options.uuids)
    const slaveRes = await slaveClient.query(query)

    if (options.safe) {
      if (slaveRes.rows.length !== 0) {
        throw new Error('some of those rows already exist on the slave db')
      }
    } else {
      const existingRows = map(slaveRes.rows, 'id')
      options.uuids = difference(options.uuids, existingRows)
    }
  }

  const moveRows = async () => {
    // No rows to copy!
    if (options.uuids.length === 0) {
      return
    }

    let query = escape('SELECT * FROM %I WHERE "id" in %L;', options.table, options.uuids)
    const res = await masterClient.query(query)

    const fields = map(res.fields, field => escape.ident(field.name))
    const values = map(res.rows, row => escape('(%s)', encodedValues(row)))
    query = escape('INSERT INTO %I (%s) VALUES ', options.table, fields)
    query = query + values.join(', ')
    console.log(query)

    await slaveClient.query(query)

    if (!options.clone) {
      // warning: if this request fails the rows will still be in the slave db
      await deleteRows()
    }
  }

  const encodedValues = function (vals) {
    return map(vals, (v) => {
      if (typeof (v) === 'number') { return v }
      if (v !== null && v.constructor.name === 'Date') { return escape.literal(v.toISOString()) }
      if (v !== null && v.constructor.name === 'Boolean') { return escape.string(v) }
      if (v !== null && v.constructor.name === 'Array') { return v.replace("('", "{").replace("')", "}") }
      return escape.literal(v)
    })
  }

  const deleteRows = async () => {
    const query = escape('DELETE FROM %I WHERE "id" in %L;', options.table, options.uuids)
    await masterClient.query(query)
  }

  const closeDBConnections = async () => {
    await masterClient.end()
    await slaveClient.end()
  }

  return suck(event)
    .then(() => {
      const response = {
        statusCode: 200,
        headers: {},
        body: JSON.stringify({ count: options.uuids.length }),
        isBase64Encoded: false
      }
      return response
    })
    .catch((e) => {
      closeDBConnections()
      throw e
    })
}
