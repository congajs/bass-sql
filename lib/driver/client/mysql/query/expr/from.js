/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var BaseFrom = require('../../../../query/expr/From');

/**
 * From class
 *
 * @param {String} from The table name
 * @param {String} alias The alias for the table name
 * @param {String} index The index the from should use
 * @constructor
 */
function From(from, alias, index) {

	BaseFrom.apply(this, [from, alias]);

	this._index = index;
}
From.prototype = new BaseFrom();
From.prototype.constructor = From;

/**
 * The index to use
 * @type {String|*}
 * @private
 */
From.prototype._index = null;

/**
 * Set the index to use
 * @param {String|*} index
 */
From.prototype.setIndex = function(index) {
	this._index = index;
};

/**
 * Get the index to use
 * @returns {String|*}
 */
From.prototype.getIndex = function() {
	return this._index;
};

/**
 * {@inheritdoc}
 */
From.prototype.toString = function() {

	var str = BaseFrom.prototype.toString.call(this);

	var index = this.getIndex();
	if (index) {
		str += ' USE INDEX ' + index;
	}

	return str;
};



module.exports = From;