/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * From class
 *
 * @param {String} from The table name
 * @param {String} alias The alias for the table name
 * @constructor
 */
function From(from, alias) {

	this._from = from;
	this._alias = alias;
}



From.prototype = {
	/**
	 * The table name
	 *
	 * @var {String}
	 */
	_from: null ,

	/**
	 * The table alias
	 *
	 * @var {String}
	 */
	_alias: null ,

	/**
	 * Get the table name
	 *
	 * @returns {String}
	 */
	getFrom: function() {
		return this._from;
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
	 * Get the string representation of this expression
	 *
	 * @returns {String}
	 */
	toString: function() {
		var str = this.getFrom();

		var alias = this.getAlias();
		if (alias)
		{
			str += ' ' + alias;
		}

		return str;
	}
};



From.prototype.constructor = From;

module.exports = From;