/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

module.exports = {

	/**
	 * Get a QueryBuilder for a Bass Manager
	 *
	 * @param {ManagerDefinition} definition
	 * @param {Manager} manager
	 * @returns {QueryBuilder} Instance
	 */
	factory: function(definition, manager)
	{
		var _class = this.getClass(definition);
		return new _class(manager);
	} ,

	/**
	 * Get a QueryBuilder Class for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {QueryBuilder} Constructor
	 */
	getClass: function(definition)
	{
		var _class;
		switch (definition.driver)
		{
			case 'mysql' :
			{
				_class = require('./driver/client/mysql/query-builder');
				break;
			}
			default :
			{
				_class = require('./query-builder');
				break;
			}
		}
		return _class;
	}
};