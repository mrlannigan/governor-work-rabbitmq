'use strict';

var BPromise = require('bluebird');

function RabbitMQEngine() {
    this.rabbit = null;
}


/**
 * Initialize connections when the agent is created
 * @param options
 */
RabbitMQEngine.prototype.setup = function (options) {

};

/**
 * Setup job specific channel and respond to agent when work comes in
 * @param taskCallback (task, ack, nack)
 * @param options
 */
RabbitMQEngine.prototype.consume = function (taskCallback, options) {

};

exports.engine = RabbitMQEngine;
