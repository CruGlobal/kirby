'use strict';

console.log('Loading function');

exports.handler = function(event, context, callback) {
    callback(null, "some success message");
};