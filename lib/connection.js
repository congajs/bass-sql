/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Connection = function(connection){
	this.connection = connection;
};

Connection.prototype = {

	/**
	 * 
	 * @param  {Metadata} metadataRegistry
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot: function(metadataRegistry, cb){
		if (cb){
			cb(null);			
		}
	},

	/**
	 * Close the connection
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	close: function(cb){
		this.connection.end(function(err){

			if (cb){
				cb(err);
			}

		});
	}

};

module.exports = Connection;