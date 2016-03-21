'use strict';

var Hoek = require('hoek-boom'),
    factory = require('./factory').factory,
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

    this.rabbit = factory(this.options);

    _createConnection = this.rabbit.createConnection;
    this.rabbit.createConnection = function () {
        return _createConnection.apply(self.rabbit, arguments).tap(function () {
            self.emit('connected');
        });
    };
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
    Hoek.assert(typeof options.fixture === 'function', 'options.fixture should be a function');

    if (options.retry) {
        Hoek.assert(options.retry.queue && options.retry.queue.length, 'options.retry.queue should be a string with a length');
        Hoek.assert(options.retry.dead_letter_exchange && options.retry.dead_letter_exchange.length, 'options.retry.dead_letter_exchange should be a string with a length');
        Hoek.assert(options.retry.message_ttl && options.retry.message_ttl > 100, 'options.retry.message_ttl should be a number greater than 100ms');
    }

    this.rabbit.getChannel(function (channel, release) {
        var consumerTag,
            consumedQueue,
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
                task.meta = {
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

            nack = function (requeueAction) {
                if (singleCall) {
                    self.emit('error', new Error('Called nack more than once or after a ack has been called'));
                    return;
                }

                var requeue = true;
                singleCall = true;

                if (channel.isClosed) {
                    self.emit('orphaned-nack', task);
                    return;
                }

                if (requeueAction === 'deadletter') {
                    requeue = false;
                }

                if (requeueAction === 'tailqueue') {
                    task.tailqueued = (task.tailqueued || 0) + 1;
                    task.last_tailqueued = (new Date()).toISOString();
                    delete task.meta;
                    channel.sendToQueue(consumedQueue, new Buffer(JSON.stringify(task)));

                    requeue = false;
                }

                channel.nack(message, false, requeue);
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
            consumedQueue = consumer.queue;
        });
    }, null, function (err) {
        self.emit('error', err);
    });

    return resultAPI;
};

exports.engine = RabbitMQEngine;
