'use strict';

console.log('Loading function');

const options = {
    // the clone option sets if Kirby will leave the rows in the master DB
    // if false, the rows will be deleted from master after the copy
    clone: true,
    // the safe option sets if Kirby will fail if some of uuid's exist in the slave table
    safe: true,
    table: '',
    uuids: []
};

const { Pool, Client } = require('pg');
const async = require('asyncawait/async');
const await = require('asyncawait/await');
const _ = require('lodash/core');
const _array = require('lodash/array');
const values = require('lodash/values')
const escape = require('pg-escape');

const masterClient = new Client({
    user: process.env.MASTER_PG_USER,
    host: process.env.MASTER_PG_ADDR,
    database: process.env.MASTER_PG_DB,
    password: process.env.MASTER_PG_PASS
});
const slaveClient = new Client({
    user: process.env.SLAVE_PG_USER,
    host: process.env.SLAVE_PG_ADDR,
    database: process.env.SLAVE_PG_DB,
    password: process.env.SLAVE_PG_PASS
});

exports.handler = function(event, context, callback) {
    suck(event)
        .then(() => {
            callback(null, "some success message");
        })
        .catch((e) => {
            closeDBConnections();
            callback(e);
        });
};

const suck = async (function (event) {
    options.table = event.table;
    options.uuids = event.uuids.split(/,/);

    if(event["clone"] !== undefined)
        options.clone = event.clone;

    if(event["safe"] !== undefined)
        options.safe = event.safe;

    await (connectToDBs());

    await (checkTables());

    await (checkRows());

    await (moveRows());

    await (closeDBConnections());
});

const connectToDBs = async (function () {
    await (masterClient.connect());
    await (masterClient.query('SELECT NOW()'));

    await (slaveClient.connect());
    await (slaveClient.query('SELECT NOW()'));
});

// checking that table exists both places
const checkTables = async (function() {
    const query = "SELECT 1 " +
        "FROM information_schema.tables " +
        "WHERE table_schema='public' " +
        "AND table_type='BASE TABLE'" +
        "AND table_name = %L;";

    const checks = _.map([masterClient, slaveClient], async ((client) => {
        const escaped = escape(query, options.table);
        const res = await (client.query(escaped));
        if(res.rows.length === 0) {
            throw 'Master DB missing table: ' + options.table
        }
    }));

    await(checks);
});

// checking that the rows exist in master db
const checkRows = async (function() {
    const expectedCount = _array.uniq(options.uuids).length;

    let query = escape("SELECT COUNT(*) FROM %I WHERE \"uuid\" in %L;", options.table, options.uuids);

    const masterRes = await (masterClient.query(query));
    const diff = expectedCount - parseInt(masterRes.rows[0].count, 10);
    if(diff !== 0) {
        throw diff + ' uuids could not be found.';
    }

    // checking that the rows do not exist in slave db
    query = escape("SELECT uuid FROM %I WHERE \"uuid\" in %L;", options.table, options.uuids);
    const slaveRes = await (slaveClient.query(query));

    if(options.safe) {
        if(slaveRes.rows.length !== 0) {
            throw 'some of those rows already exist on the slave db';
        }
    }
    else {
        const existingRows = _.map(slaveRes.rows, 'uuid');
        options.uuids = _array.difference(options.uuids, existingRows);
    }
});

const moveRows = async (function() {
    // No rows to copy!
    if(options.uuids.length === 0) {
        return;
    }

    const query = escape("SELECT * FROM %I WHERE \"uuid\" in %L;", options.table, options.uuids);
    const res = await (masterClient.query(query));

    try {
        await (slaveClient.query('BEGIN'));

        const fields = _.map(res.fields, 'name');
        const values = _.map(res.rows, (row) => { return escape("(%s)", encodedValues(row)) });
        let query = escape("INSERT INTO %I (%s) VALUES ", options.table, fields);
        query = query + values.join(', ');

        await (slaveClient.query(query));

        if(!options.clone)
            await (deleteRows());
        await (slaveClient.query('COMMIT'));
    } catch (e) {
        await (slaveClient.query('ROLLBACK'));
        throw e;
    }
});

const encodedValues = function(vals) {
    return _.map(vals, (v) => {
        if(typeof(v) == 'number')
            return v;
        if(v !== null && v.constructor.name == 'Date')
            return escape.literal(v.toISOString());
        if(v !== null && v.constructor.name == 'Boolean')
            return escape.string(v);
        return escape.literal(v);
    })
}

const deleteRows = async (function() {
    const query = escape("DELETE FROM %I WHERE \"uuid\" in %L;", options.table, options.uuids);
    await (masterClient.query(query));
});

const closeDBConnections = async (function () {
    await (masterClient.end());
    await (slaveClient.end());
});
