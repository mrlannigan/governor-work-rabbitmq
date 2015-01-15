'use strict';

var Hoek = require('hoek-boom'),
    BPromise = require('bluebird'),
    genericPool = require('generic-pool').Pool;

function RabbitMQPool (conf, connection, poolOptions) {
    var pool,
        tempRelease;

    Hoek.assert(conf, 'Invalid configuration for rabbitmq data source');

    if (!poolOptions) {
        poolOptions = {};
    }

    if (!poolOptions.min || (poolOptions.hasOwnProperty('min') && poolOptions.min < 1)) {
        poolOptions.min = 0;
    }

    if (!poolOptions.max || (poolOptions.hasOwnProperty('max') && poolOptions.max < 1)) {
        poolOptions.max = 1;
    }

    if (!poolOptions.hasOwnProperty('refreshIdle') || typeof poolOptions.refreshIdle !== 'boolean') {
        poolOptions.refreshIdle = false;
    }

    if (!poolOptions.idleTimeoutMillis || (poolOptions.hasOwnProperty('idleTimeoutMillis') && poolOptions.idleTimeoutMillis < 1)) {
        poolOptions.idleTimeoutMillis = 30000;
    }

    if (!poolOptions.reapIntervalMillis || (poolOptions.hasOwnProperty('reapIntervalMillis') && poolOptions.reapIntervalMillis < 1)) {
        poolOptions.reapIntervalMillis = 10000;
    }

    if (!poolOptions.hasOwnProperty('log')) {
        poolOptions.log = false;
    }

    pool = genericPool({
        name: 'rabbitmq-' + name + '-' + Math.floor(Math.random() * (9999 - 1001) + 1000),

        create: function (callback) {
            connection.createChannel()
                .then(function (channel) {
                    channel.on('error', function (err) {
                        channel.isClosed = true;
                        channel.error = err;
                    });
                    channel.on('close', function () {
                        channel.isClosed = true;
                    });
                    callback(null, channel);
                })
                .catch(function (err) {
                    callback(err, null);
                });
        },

        destroy: function (channel) {
            if (channel) {
                channel.isClosed = true;
                channel.close();
            }
        },

        max: poolOptions.max,
        min: poolOptions.min,
        idleTimeoutMillis: poolOptions.idleTimeoutMillis,
        reapIntervalMillis: poolOptions.reapIntervalMillis,
        refreshIdle: poolOptions.refreshIdle,
        log: poolOptions.log,

        validate: function (channel) {
            return channel && !channel.isClosed;
        }
    });

    pool.acquire = BPromise.promisify(pool.acquire, pool);

    tempRelease = pool.release;
    pool.release = function (channel) {
        tempRelease.call(pool, channel);
        return BPromise.resolve();
    };

    connection.on('close', function () {
        pool.drain(function () {
            pool.destroyAllNow();
        });
    });

    return pool;
}

module.exports = RabbitMQPool;
