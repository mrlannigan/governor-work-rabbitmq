'use strict';

var BPromise = require('bluebird'),
    Hoek = require('hoek-boom'),
    Factory = require('./factory').factory;

/**
 * Construct engine object
 * @param options
 * @constructor
 */
function RabbitMQEngine(options) {
    this.options = options;
    this.rabbit = null;
}

/**
 * Initialize connections when the agent is created
 */
RabbitMQEngine.prototype.setup = function () {
    this.rabbit = Factory(this.options);
};

/**
 * Setup job specific channel and respond to agent when work comes in
 * @param options
 * @param taskCallback (task, ack, nack)
 */
RabbitMQEngine.prototype.consume = function (options, taskCallback) {
    var resultAPI = {};

    Hoek.assert(options, 'options must be defined');
    Hoek.assert(typeof options.fixture == 'function', 'options.fixture should be a function');

    this.rabbit.getChannel(function (channel, release) {
        var consumerTag,
            consumeCallback;

        resultAPI.cancel = function () {
            return channel.cancel(consumerTag).then(function () {
                return release();
            });
        };

        consumeCallback = function (message) {
            var task,
                ack,
                nack;

            try {
                task = JSON.parse(message.content.toString());
                task._meta = message.fields;
            } catch (e) {
                // invalid task content... what todo?
                task = {error: e, message: message};
            }

            ack = function () {
                channel.ack(message);
            };

            nack = function () {
                channel.nack(message);
            };

            taskCallback(task, ack, nack);
        };

        return options.fixture(channel, consumeCallback);
    });

    return resultAPI;
};

exports.engine = RabbitMQEngine;
