/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var BassConnection = require('bass').Connection;

function Connection(connection){
	BassConnection.apply(this, arguments);
}
Connection.prototype = new BassConnection();
Connection.prototype.constructor = Connection;

/**
 * Close the connection
 *
 * @param  {Function} cb
 * @return {void}
 */
Connection.prototype.close = function(cb) {
	this.connection.end(function(err) {

		if (cb){
			cb(err);
		}

	});
};

module.exports = Connection;