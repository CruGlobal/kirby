'use strict';

console.log('Loading function');

var clone = true;

const { Pool, Client } = require('pg');
var async = require('asyncawait/async');
var await = require('asyncawait/await');

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
            callback(e);
        });
};

var run = async (function (event) {
    const table = event.table;
    const uuids = event.uuids.split(/,/);

    if(event["clone"] !== undefined)
        clone = event.clone;

    await (connectToDBs());

    checkTables(table);

    checkRows(table, uuids);

    moveRows(table, uuids);

    await (closeDBConnections());
});

var connectToDBs = async (function () {
    p('-- connecting to databases');

    await (masterClient.connect());

    const res = await (masterClient.query('SELECT NOW()'));
    p('master now: ' + res.rows[0].now);

    await (slaveClient.connect());
    await (slaveClient.query('SELECT NOW()'));
});

function checkTables(table) {
    p('-- checking that table exists both places');
}

function checkRows(table, uuids) {
    p('-- checking that the rows exist in master db');
}

function moveRows(table, uuids) {
    p('-- copying rows to slave db');
    if(!clone)
        deleteRows(table, uuids);
}

function deleteRows(table, uuids) {
    p('-- deleting rows from master db');
}

var closeDBConnections = async (function () {
    p('-- closing connections to databases');

    await (masterClient.end());
    await (slaveClient.end());
});

function p(s) {
    console.log(s)
}
