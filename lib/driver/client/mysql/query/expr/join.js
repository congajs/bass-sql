/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var BaseJoin = require('../../../../query/expr/Join');

/**
 * Join class
 *
 * @param {String} type The join type INNER or LEFT
 * @param {String} table The table name you are joining
 * @param {String} alias The table alias
 * @param {String} condition The condition to join ON
 * @param {String} index The index to use
 * @constructor
 */
function Join(type, table, alias, condition, index) {

	BaseJoin.apply(this, [type, table, alias, condition]);

	this._index = index;
}
Join.prototype = new BaseJoin();
Join.prototype.constructor = Join;

/**
 * The index to use
 * @type {String|*}
 * @private
 */
Join.prototype._index = null;

/**
 * Get the index to use
 * @returns {String|*}
 */
Join.prototype.getIndex = function() {
	return this._index;
};

/**
 * {@inheritdoc}
 */
Join.prototype.toString = function() {
	var str = this.getType().toUpperCase() + ' JOIN ' + this.getTable();

	var alias = this.getAlias();
	if (alias) {
		str += ' ' + alias;
	}

	var index = this.getIndex();
	if (index) {
		str += ' USE INDEX ' + index;
	}

	var condition = this.getCondition();
	if (condition) {
		str += ' ON ' + condition;
	}

	return str;
};



module.exports = Join;