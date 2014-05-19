/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var anyDb      = require('any-db');
var Connection = require('./connection');

module.exports = {

	currentConnection: null,

	/**
	 * Configure and build a connection
	 * 
	 * @param  {Object}   config
	 * @param  {Function} cb
	 * @return {void}
	 */
	factory: function(config, cb){

		var self = this;

		try {

			var conn = self.createConnection(config);

			conn.on('error', function(err){

				if (err.code === 'PROTOCOL_CONNECTION_LOST'){
					
					self.currentConnection.connection = self.createConnection(config);

				} else {
					throw err;
				}

			});

			conn.on('close', function(err){
				if (err){
					console.log(err);
				}
			});

			this.currentConnection = new Connection(conn);

			cb(null, this.currentConnection);

		} catch (e) {
			cb(e, null);
		}
	},

	/**
	 * Create a connection from a config object
	 * 
	 * @param  {Object} config
	 * @return {Object}
	 */
	createConnection: function(config)
	{
		if (config.password === null){
			config.password = '';
		}

		var conn;
		var url = config.driver + '://' + config.user + ':' + config.password + 
					'@' + config.host + ':' + config.port + '/' + config.database;

		// create connection pool or single connection
		if (typeof config.pool.enabled !== 'undefined' && config.pool.enabled === true){
			conn = anyDb.createPool(url, config.pool);
		} else {
			conn = anyDb.createConnection(url);
		}

		return conn;
	}
};