'use strict';

console.log('Loading function');

var clone = true;

const { Pool, Client } = require('pg');

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
    const table = event.table;
    const uuids = event.uuids.split(/,/);

    if(event["clone"] !== undefined)
        clone = event.clone;

    connectToDatabases();

    checkTables(table);

    checkRows(table, uuids);

    moveRows(table, uuids);

    callback(null, "some success message");
};

function connectToDatabases() {
    p('connecting to databases');
    masterClient.connect();

    masterClient.query('SELECT NOW()')
                .then(res => {
                    p('master now: ' + res.rows[0].now);
                    masterClient.end();
                })
                .catch(db_connection_catch);

    slaveClient.connect();

    slaveClient.query('SELECT NOW()')
               .then(res => {
                   p('slave now: ' + res.rows[0].now);
                   slaveClient.end();
               })
               .catch(db_connection_catch);
}

function db_connection_catch(e) {
    console.error(e.stack)
}

function checkTables(table) {
    p('checking that table exists both places');
}

function checkRows(table, uuids) {
    p('checking that the rows exist in master db');
}

function moveRows(table, uuids) {
    p('copying rows to slave db');
    if(!clone)
        deleteRows(table, uuids);
}

function deleteRows(table, uuids) {
    p('deleting rows from master db');
}

function p(s) {
    console.log(s)
}
