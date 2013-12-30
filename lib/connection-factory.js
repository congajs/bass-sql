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

	/**
	 * Configure and build a connection
	 * 
	 * @param  {Object}   config
	 * @param  {Function} cb
	 * @return {Void}
	 */
	factory: function(config, cb){

		try {

			if (config.password === null){
				config.password = '';
			}

			var conn;
			var url = config.driver + '://' + config.user + ':' + config.password + 
						'@' + config.host + ':' + config.port + '/' + config.database;

			// create connection pool or single connection
			if (typeof config.pool.enabled !== 'undefined' && config.pool.enabled === true){
				conn = anyDb.createPool(url, { min : config.pool.min, max : config.pool.max });
			} else {
				conn = anyDb.createConnection(url);
			}

			cb(null, new Connection(conn));

		} catch (e) {

			cb(e, null);
		}
	}
};