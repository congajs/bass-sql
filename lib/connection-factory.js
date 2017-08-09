/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const anyDb = require('any-db');
const Connection = require('./connection');

module.exports = class ConnectionFactory {

    /**
	 * Get the current connection (if any)
     * @returns {*}
     */
	static get currentConnection() {
        return ConnectionFactory.__conn;
	}

    /**
	 * Set the current connection
     * @param conn
     * @returns {*}
     */
	static set currentConnection(conn) {
		return ConnectionFactory.__conn = conn;
	}

	/**
	 * Configure and build a connection
	 * 
	 * @param  {Object}   config
	 * @param  {Function} cb
	 * @return {void}
	 */
	static factory(config, logger, cb) {

		try {

			const conn = this.createConnection(config);

			conn.on('error', err => {

				if (err.code === 'PROTOCOL_CONNECTION_LOST') {
					
					this.currentConnection.connection = this.createConnection(config);

				} else {
					throw err;
				}

			});

			conn.on('close', err => {
				if (err) {
					console.error(err.stack || err);
				}
			});

			this.currentConnection = new Connection(conn);

			cb(null, this.currentConnection);

		} catch (e) {

			cb(e, null);
		}
	}

	/**
	 * Create a connection from a config object
	 * 
	 * @param  {Object} config
	 * @return {Object}
	 */
	static createConnection(config) {
		if (config.password === null){
			config.password = '';
		}

		const url = config.driver + '://' + config.user + ':' + config.password +
					'@' + config.host + ':' + config.port + '/' + config.database;

        let conn;

		// create connection pool or single connection
		if (config.pool instanceof Object && config.pool.enabled === true) {
			conn = anyDb.createPool(url, config.pool);
		} else {
			conn = anyDb.createConnection(url);
		}

		return conn;
	}
};