/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Join class
 *
 * @param {String} type The join type INNER or LEFT
 * @param {String} table The table name you are joining
 * @param {String} alias The table alias
 * @param {String} condition The condition to join ON
 * @constructor
 */
function Join(type, table, alias, condition) {

	this._type = type;
	this._table = table;
	this._alias = alias;
	this._condition = condition;
}


/**
 * Constant for INNER JOIN
 *
 * @type {String}
 */
Join.TYPE_INNER = 'INNER';

/**
 * Constant for LEFT JOIN
 *
 * @type {String}
 */
Join.TYPE_LEFT = 'LEFT';



Join.prototype = {
	/**
	 * The join type INNER or LEFT
	 *
	 * @type {String}
	 */
	_type: null ,

	/**
	 * The table name
	 *
	 * @type {String}
	 */
	_table: null ,

	/**
	 * The table alias
	 *
	 * @type {String}
	 */
	_alias: null ,

	/**
	 * The condition used to join
	 *
	 * @type {String}
	 */
	_condition: null ,

	/**
	 * Get the join type
	 *
	 * @returns {String}
	 */
	getType: function() {
		return this._type;
	} ,

	/**
	 * Get the table name
	 *
	 * @returns {String}
	 */
	getTable: function() {
		return this._table;
	} ,

	/**
	 * Get the table alias
	 *
	 * @returns {String}
	 */
	getAlias: function() {
		return this._alias;
	} ,

	/**
	 * Get the join condition
	 *
	 * @returns {String}
	 */
	getCondition: function() {
		return this._condition;
	} ,

	/**
	 * Get a string representation of this expression
	 *
	 * @returns {String}
	 */
	toString: function() {
		var str = this.getType().toUpperCase() + ' JOIN ' + this.getTable();

		var alias = this.getAlias();
		if (alias) {
			str += ' ' + alias;
		}

		var condition = this.getCondition();
		if (condition) {
			str += ' ON ' + condition;
		}

		return str;
	}
};



Join.prototype.constructor = Join;

module.exports = Join;