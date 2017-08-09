/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const BassConnection = require('bass').Connection;

class Connection extends BassConnection {

    /**
     * Close the connection
     *
     * @param  {Function} cb
     * @return {void}
     */
	close(cb) {
		this.connection.end(err => {
			if (typeof cb === 'function') {
				cb(err);
			}
		});
	}

}

module.exports = Connection;