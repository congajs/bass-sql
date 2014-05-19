/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// local modules
var ClientClass = require('./client');
var MysqlClientClass = require('./driver/client/mysql/mysql-client');

module.exports = {

	/**
	 * Get a bass Client for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {Client} Instance
	 */
	factory: function(definition)
	{
		var _class = this.getClass(definition);
		return new _class(definition.connection, definition.logger);
	} ,

	/**
	 * Get a bass Client Class for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {Client} Constructor
	 */
	getClass: function(definition)
	{
		var _class;
		switch (definition.driver)
		{
			case 'mysql' :
			{
				_class = MysqlClientClass;
				break;
			}
			default :
			{
				_class = ClientClass;
				break;
			}
		}
		return _class;
	}
};