/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var AbstractExpr = require('./abstract-expr');

/**
 * OrderBy Class for building order-by query parts
 *
 * @param {String} sort
 * @param {String} order
 * @constructor
 */
function OrderBy(sort, order) {

	AbstractExpr.apply(this, [[sort, order]]);
}
OrderBy.prototype = new AbstractExpr();
OrderBy.prototype.constructor = OrderBy;

/**
 * {@inheritdoc}
 */
OrderBy.prototype.preSeparator = '';

/**
 * {@inheritdoc}
 */
OrderBy.prototype.postSeparator = '';

/**
 * {@inheritdoc}
 */
OrderBy.prototype.addMultiple = function(args) {

	if (!args) {
		return this;
	}

	if (arguments.length === 2) {
		args = Array.prototype.slice.call(arguments);
	}

	if (!Array.isArray(args)) {
		args = [args];
	} else if (args.length > 0 && !Array.isArray(args[0])) {
		if (args.length > 1) {
			args = [ [args[0], args[1]] ];
		} else {
			args = [ [args[0]] ];
		}
	}

	var i, len = args.length;

	for (i = 0; i < len; i++) {
		if (Array.isArray(args[i]) && args[i].length > 1) {

			this.add(args[i][0], args[i][1]);
		} else {

			this.add(args[i][0]);
		}
	}

	return this;
};

/**
 * {@inheritdoc}
 */
OrderBy.prototype.add = function(sort, order) {

	this.parts.push( sort + ' ' + (order || 'ASC') );

	return this;
};

/**
 * {@inheritdoc}
 */
OrderBy.prototype.toString = function() {

	return this.preSeparator +
		this.parts.join(this.separator) +
		this.postSeparator;
};



module.exports = OrderBy;