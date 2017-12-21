'use strict';

console.log('Loading function');

var clone = true;

exports.handler = function(event, context, callback) {
    const table = event.table;
    const uuids = event.uuids.split(/,/);

    if(event["clone"] !== undefined)
        clone = event.clone;

    p(table);
    p(uuids);

    connectToDatabases();

    checkTables(table);

    checkRows(table, uuids);

    moveRows(table, uuids);

    callback(null, "some success message");
};

function connectToDatabases() {
    p('connecting to databases');
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
