'use strict';

console.log('Loading function');

let clone = true;

const { Pool, Client } = require('pg');
const async = require('asyncawait/async');
const await = require('asyncawait/await');
const _ = require('lodash/core');

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

    checkRows(table, uuids);

    moveRows(table, uuids);

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

    let query = "SELECT 1 " +
        "FROM information_schema.tables " +
        "WHERE table_schema='public' " +
        "AND table_type='BASE TABLE'" +
        "AND table_name = $1;";

    let checks = _.map([masterClient, slaveClient], async ((client) => {
        const res = await (client.query(query, [table]));
        if(res.rows.length === 0) {
            throw 'Master DB missing table: ' + table
        }
    }));

    await(checks);
});

let checkRows = async (function(table, uuids) {
    p('-- checking that the rows exist in master db');
});

function moveRows(table, uuids) {
    p('-- copying rows to slave db');
    if(!clone)
        deleteRows(table, uuids);
}

function deleteRows(table, uuids) {
    p('-- deleting rows from master db');
}

let closeDBConnections = async (function () {
    p('-- closing connections to databases');

    await (masterClient.end());
    await (slaveClient.end());
});

function p(s) {
    console.log(s)
}
