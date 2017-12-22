'use strict';

console.log('Loading function');

let clone = true;

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
    run(event)
        .then(() => {
            callback(null, "some success message");
        })
        .catch((e) => {
            closeDBConnections();
            callback(e);
        });
};

var run = async (function (event) {
    const table = event.table;
    const uuids = event.uuids.split(/,/);

    if(event["clone"] !== undefined)
        clone = event.clone;

    await (connectToDBs());

    await (checkTables(table));

    await (checkRows(table, uuids));

    await (moveRows(table, uuids));

    await (closeDBConnections());
});

let connectToDBs = async (function () {
    p('-- connecting to databases');

    await (masterClient.connect());

    const res = await (masterClient.query('SELECT NOW()'));
    p('master now: ' + res.rows[0].now);

    await (slaveClient.connect());
    await (slaveClient.query('SELECT NOW()'));
});

let checkTables = async (function(table) {
    p('-- checking that table exists both places');

    const query = "SELECT 1 " +
        "FROM information_schema.tables " +
        "WHERE table_schema='public' " +
        "AND table_type='BASE TABLE'" +
        "AND table_name = %L;";

    let checks = _.map([masterClient, slaveClient], async ((client) => {
        const escaped = escape(query, table);
        const res = await (client.query(escaped));
        if(res.rows.length === 0) {
            throw 'Master DB missing table: ' + table
        }
    }));

    await(checks);
});

let checkRows = async (function(table, uuids) {
    p('-- checking that the rows exist in master db');

    const expectedCount = _array.uniq(uuids).length;

    const query = escape("SELECT COUNT(*) FROM %I WHERE \"uuid\" in %L;", table, uuids);

    const res = await (masterClient.query(query));
    const diff = expectedCount - parseInt(res.rows[0].count, 10)
    if(diff !== 0) {
        throw diff + ' uuids could not be found.'
    }

    // TODO: Ensure uuid's don't already exist on slave
});

let moveRows = async (function(table, uuids) {
    p('-- copying rows to slave db');

    const query = escape("SELECT * FROM %I WHERE \"uuid\" in %L;", table, uuids);
    const res = await (masterClient.query(query));

    // TODO - open transaction
    let pushes = _.map(res.rows, async ((row) => {
        await (moveRow(table, row));
    }));
    await(pushes);

    if(!clone)
        await (deleteRows(table, uuids));
    // TODO - close transaction
});

let moveRow = async (function(table, row) {
    let vals = values(row);
    vals = encodedValues(vals);
    const query = escape("INSERT INTO %I VALUES(%s)", table, vals);
    await (slaveClient.query(query));
});

let encodedValues = function(vals) {
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

let deleteRows = async (function(table, uuids) {
    p('-- [TODO] deleting rows from master db');
});

let closeDBConnections = async (function () {
    p('-- closing connections to databases');

    await (masterClient.end());
    await (slaveClient.end());
});

function p(s) {
    console.log(s)
}
