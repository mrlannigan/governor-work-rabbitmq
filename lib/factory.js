'use strict';

var Hoek = require('hoek-boom'),
    BPromise = require('bluebird'),
    amqp = require('amqplib'),
    RabbitMQPool = require('./pool').pool;

function RabbitMQFactory (conf) {
    var rabbit = {},
        log;

    Hoek.assert(conf, 'Invalid configuration for rabbitmq data source');

    log = conf.log;
    delete conf.log;
    Hoek.assert(typeof log === 'object' && typeof log.warn === 'function' && typeof log.error === 'function', 'Provided log must contain warn and error properties');

    rabbit.connection = null;
    rabbit.pool = null;
    rabbit.log = log;

    if (conf.pool && conf.pool.hasOwnProperty('log') && conf.pool.log === true) {
        conf.pool.log = function (msg, level) {
            level = level == 'verbose' ? 'debug' : level;
            rabbit.log[level] && rabbit.log[level](msg);
        }
    }

    rabbit.createConnection = function () {
        if (rabbit.connection) {
            return BPromise.resolve(rabbit.connection);
        }

        var catcher = function (returnRejection) {
            return function ConnectionErrorHandler (err) {
                rabbit.connection = null;

                if (rabbit.pool) {
                    rabbit.pool.destroyAllNow();
                    rabbit.pool = null;
                }

                if (returnRejection) {
                    return BPromise.reject(err);
                }
            }
        };

        return amqp.connect(conf.connectionUrl, conf.socketOptions)
            .then(function (connection) {
                rabbit.connection = connection;

                connection.on('error', catcher(false));

                return connection;
            })
            .catch(catcher(true));
    };

    rabbit.createPool = function (poolOptions) {
        Hoek.assert(this.connection, 'Connection must be created first before creating the pool');

        if (!rabbit.pool) {
            rabbit.pool = RabbitMQPool(conf, this.connection, poolOptions);
        }

        return BPromise.resolve(rabbit.pool);
    };

    /**
     * This function will attempt to capture failures within the rabbit connection or channel and
     * automatically retry the fixture with a new channel/connection.
     * @param {function} fixture (channel, release){} Must call release when done with the channel
     * @param {object} [log] defaults to the logger given at factory generation
     * @param {function} errorHandler called when unable to retrieve channel
     * @returns {BPromise}
     */
    rabbit.getChannel = function (fixture, log, errorHandler) {
        log = log || rabbit.log;

        Hoek.assert(typeof fixture === 'function', 'Provided fixture must be a function');
        Hoek.assert(typeof log === 'object' && typeof log.warn === 'function' && typeof log.error === 'function', 'Provided log must contain warn and error properties');

        var wrappedFixture,
            tryCount = 0,
            failureCount = 10,
            initalBackOffDelay = 50,
            backOffDelay = initalBackOffDelay,
            maxDelayInterval = 5000;

        wrappedFixture = function () {
            var ok,
                channelRelease,
                connectionError,
                channelError,
                connection,
                channel,
                errorOccurred = false,
                rejectError,
                retry;

            ++tryCount;

            retry = function (err) {
                if (tryCount > failureCount) {
                    channelRelease();
                    rejectError = new Error('Reached max attempts of ' + failureCount);
                    rejectError.engine = err;
                    errorHandler(rejectError);
                    return;
                }

                setTimeout(function () {
                    wrappedFixture();
                }, backOffDelay);

                log.warn({err: err}, 'Failed ' + tryCount + ' times ... waiting ' + backOffDelay + 'ms before retrying...');

                // fibonacci backoff strategy
                if (maxDelayInterval > backOffDelay) {
                    backOffDelay += backOffDelay
                }
            };

            channelRelease = function () {
                if (connection) {
                    connection.removeListener('error', connectionError);
                    connection.removeListener('close', connectionError);
                }
                if (channel) {
                    channel.removeListener('error', channelError);
                    channel.removeListener('close', channelError);
                }

                if (rabbit.pool) {
                    rabbit.pool.release(channel);
                }

                return BPromise.resolve();
            };

            ok = BPromise.resolve().cancellable().then(function () {
                return rabbit.createConnection().catch(function (err) {
                    errorOccurred = true;
                    retry(err);
                    ok.cancel(err);
                });
            });

            ok = ok.delay(10).then(function (conn) {
                connection = conn;

                connectionError = function (err) {
                    if (errorOccurred) {
                        return;
                    }

                    errorOccurred = true;
                    connection.removeListener('error', connectionError);
                    connection.removeListener('close', connectionError);

                    retry();
                };

                connection.on('error', connectionError);
                connection.on('close', connectionError);
            });

            ok = ok.then(function () {
                return rabbit.createPool(conf.pool);
            });

            ok = ok.then(function (pool) {
                return pool.acquire();
            });

            ok = ok.then(function (ch) {
                channel = ch;

                channelError = function (err) {
                    if (errorOccurred) {
                        return;
                    }

                    errorOccurred = true;
                    channel.removeListener('error', channelError);
                    channel.removeListener('close', channelError);

                    retry();
                };

                channel.on('error', channelError);
                channel.on('close', channelError);

                // reset retry state because we connected
                tryCount = 0;
                backOffDelay = initalBackOffDelay;

                return fixture(channel, channelRelease)
                    .catch(function (err) {
                        log.error({err: err}, 'fixture had an error');

                        return new BPromise(function (resolve, reject) {
                            // wait and see if another event fired because we only want to
                            // reject this catch again if this catch was caused by something
                            // other than a connection or channel error.
                            setTimeout(function () {
                                if (!errorOccurred) {
                                    reject(err);
                                } else {
                                    resolve();
                                }
                            }, 10);
                        });
                    });
            });

            ok = ok.catch(function (err) {
                // do nothing
            });
        };

        return wrappedFixture();
    };

    return rabbit;
}

exports.factory = RabbitMQFactory;
