'use strict';

var BPromise = require('bluebird'),
    Hoek = require('hoek-boom'),
    Factory = require('./factory').factory,
    Emitter = require('events').EventEmitter,
    util = require('util');

/**
 * Construct engine object
 * @param options
 * @constructor
 */
function RabbitMQEngine(options) {
    this.options = options;
    this.rabbit = null;
}

util.inherits(RabbitMQEngine, Emitter);

/**
 * Initialize connections when the agent is created
 */
RabbitMQEngine.prototype.setup = function () {
    var self = this,
        _createConnection;

    this.rabbit = Factory(this.options);

    _createConnection = this.rabbit.createConnection;
    this.rabbit.createConnection = function () {
        _createConnection.apply(this, arguments).tap(function () {
            self.emit('connected');
        });
    }
};

/**
 * Setup job specific channel and respond to agent when work comes in
 * @param options
 * @param taskCallback (task, ack, nack)
 */
RabbitMQEngine.prototype.consume = function (options, taskCallback) {
    var resultAPI = {},
        self = this;

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
                nack,
                singleCall = false;

            try {
                task = JSON.parse(message.content.toString());
                task._meta = {
                    fields: message.fields,
                    properties: message.properties,
                    job_name: options.job_name
                };
            } catch (e) {
                // invalid task content... what todo?
                task = {error: e, message: message};
            }

            ack = function () {
                if (singleCall) {
                    self.emit('error', new Error('Called ack more than once or after a nack has been called'));
                    return;
                }

                singleCall = true;

                if (channel.isClosed) {
                    self.emit('orphaned-ack', task);
                    return;
                }
                channel.ack(message);
            };

            nack = function () {
                if (singleCall) {
                    self.emit('error', new Error('Called nack more than once or after a ack has been called'));
                    return;
                }

                singleCall = true;

                if (channel.isClosed) {
                    self.emit('orphaned-nack', task);
                    return;
                }
                channel.nack(message);
            };

            taskCallback(task, ack, nack);
        };

        return options.fixture(channel, consumeCallback).delay(100).then(function (consumer) {
            if (!consumer || !consumer.consumerTag) {
                var consumerTags = Object.keys(channel.consumers);

                if (consumerTags.length < 1) {
                    var error = new Error('Fixture did not consume any queues');
                    error.job_name = options.job_name;
                    self.emit('error', error);
                    return;
                }

                consumerTag = consumerTags[0];
                return;
            }

            consumerTag = consumer.consumerTag;
        });
    }, null, function (err) {
        self.emit('error', err);
    });

    return resultAPI;
};

exports.engine = RabbitMQEngine;
