'use strict';

var BPromise = require('bluebird'),
    Hoek = require('hoek-boom'),
    Factory = require('./factory').factory;

function RabbitMQEngine() {
    this.rabbit = null;
}

/**
 * Initialize connections when the agent is created
 * @param options
 */
RabbitMQEngine.prototype.setup = function (options) {
    this.rabbit = Factory(options);
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
                task = JSON.parse(message);
            } catch (e) {
                // invalid task content... what todo?
                task = {error: e};
            }

            ack = function () {
                channel.ack(message);
            };

            nack = function () {
                channel.nack(message);
            };

            taskCallback(task, ack, nack);
        };

        options.fixture(channel, consumeCallback);
    });

    return resultAPI;
};

exports.engine = RabbitMQEngine;
