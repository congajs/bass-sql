/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * The AbstractExpr class provides base functionality for all Expressions
 *
 * @param {Array.<String|AbstractExpr>} args
 * @constructor
 */
function AbstractExpr(args) {
	if (this.constructor.name !== 'AbstractExpr') {
		if (!Array.isArray(this.allowedClasses)) {
			this.allowedClasses = [];
		}

		if (!Array.isArray(this.parts)) {
			this.parts = [];
		}

		if (args) {
			this.addMultiple(args);
		}
	}
}

AbstractExpr.prototype = {

	/**
	 * Separator for the left side of this expression
	 *
	 * @type {String}
	 */
	preSeparator: '(' ,

	/**
	 * Separator for grouped expressions
	 *
	 * @type {String}
	 */
	separator: ', ' ,

	/**
	 * Separator for the right side of this expression
	 *
	 * @type {String}
	 */
	postSeparator: ')' ,

	/**
	 * Expressions allowed to be grouped within / added to this expression
	 *
	 * @type {Array.<String>}
	 */
	allowedClasses: null ,

	/**
	 * Grouped expressions / parts that have been added to this expression
	 *
	 * @type {Array.<String|AbstractExpr>}
	 */
	parts: null ,

	/**
	 * Add multiple parts / expressions to this expression
	 *
	 * @param {Array.<String|AbstractExpr>} args
	 * @returns {AbstractExpr}
	 */
	addMultiple: function(args) {

		if (!args) {
			return this;
		}

		if (!Array.isArray(args)) {
			this.add(args);
			return this;
		}

		var i, len = args.length;
		for (i = 0; i < len; i++) {
			this.add(args[i]);
		}
		return this;
	} ,

	/**
	 * Add a single expression
	 *
	 * @param {String|AbstractExpr} arg
	 * @returns {AbstractExpr}
	 * @throws Error
	 */
	'add': function(arg) {

		if (arg !== null && (
				arg.constructor.name !== this.constructor.name ||
				arg.count() > 0 ) ) {

			if (typeof arg !== 'string') {

				if (arg.constructor.name !== 'Object' &&
					this.allowedClasses.indexOf(arg.constructor.name) === -1) {

					throw new Error('Expression of type ' + arg.constructor.name + ' is not allowed in this context.');
				}
			}

			this.parts.push(arg);
		}
		return this;
	} ,

	/**
	 * Get the number of expressions that have been added
	 *
	 * @returns {Number}
	 */
	count: function() {
		return this.parts.length;
	} ,

	/**
	 * Get the added parts / expressions
	 *
	 * @returns {Array.<String|AbstractExpr>}
	 */
	getParts: function() {
		return this.parts;
	} ,

	/**
	 * Return the string representation of this object
	 *
	 * @returns {String}
	 */
	toString: function() {

		if (this.count() == 1) {
			return this.parts[0].toString();
		}

		return this.preSeparator +
				this.parts.join(this.separator) +
				this.postSeparator;
	}
};



AbstractExpr.prototype.constructor = AbstractExpr;

module.exports = AbstractExpr;