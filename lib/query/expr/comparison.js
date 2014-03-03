/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Comparison class for building query comparisons
 *
 * @param {*} leftExpr
 * @param {String} operator
 * @param {*} rightExpr
 * @constructor
 */
function Comparison(leftExpr, operator, rightExpr) {

	this._leftExpr = leftExpr;
	this._operator = operator;
	this._rightExpr = rightExpr;
}

/**
 * Constant for equals
 *
 * @type {string}
 */
Comparison.EQ = '=';

/**
 * Constant for not-equals
 *
 * @type {string}
 */
Comparison.NEQ = '<>';

/**
 * Constant for less than
 *
 * @type {string}
 */
Comparison.LT = '<';

/**
 * Constant for less than or equal to
 *
 * @type {string}
 */
Comparison.LTE = '<=';

/**
 * Constant for greater than
 *
 * @type {string}
 */
Comparison.GT = '>';

/**
 * Constant for greater than or equal to
 *
 * @type {string}
 */
Comparison.GTE = '>=';



Comparison.prototype = {
	/**
	 * The left expression
	 *
	 * @type {*}
	 */
	_leftExpr: null ,

	/**
	 * The operator to use
	 *
	 * @type {String}
	 */
	_operator: null ,

	/**
	 * The right expression
	 *
	 * @type {*}
	 */
	_rightExpr: null ,

	/**
	 * Get the left expression
	 *
	 * @returns {*}
	 */
	getLeftExpr: function() {
		return this._leftExpr;
	} ,

	/**
	 * Get the operator
	 *
	 * @returns {String}
	 */
	getOperator: function() {
		return this._operator;
	} ,

	/**
	 * Get the right expression
	 *
	 * @returns {*}
	 */
	getRightExpr: function() {
		return this._rightExpr;
	} ,

	/**
	 * Get this comparison as a string
	 *
	 * @returns {string}
	 */
	toString: function() {

		return this.getLeftExpr() + ' ' +
				this.getOperator() + ' ' +
				this.getRightExpr();
	}
};



Comparison.prototype.constructor = Comparison;

module.exports = Comparison;