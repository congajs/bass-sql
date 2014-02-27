/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

module.exports = {
	name: "bass-sql",
	annotations: [],
	client: require('./client'),
	clientFactory: require('./client-factory'),
	connectionFactory: require('./connection-factory'),
	mapper: require('./mapper'),
	listeners: [
		{
			constructor: '',
			method: '',
			event: ''
		}
	]
};