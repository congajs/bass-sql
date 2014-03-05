/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Math class
 *
 * @param {*} leftExpr
 * @param {String} operator
 * @param {*} rightExpr
 * @constructor
 */
function Math(leftExpr, operator, rightExpr) {

	this._leftExpr = leftExpr;
	this._operator = operator;
	this._rightExpr = rightExpr;
}



Math.prototype = {
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

		var leftExpr = this.getLeftExpr().toString();
		if (this.getLeftExpr().constructor.name === 'Math') {
			leftExpr = '(' + leftExpr + ')';
		}

		var rightExpr = this.getRightExpr().toString();
		if (this.getRightExpr().constructor.name === 'Math') {
			rightExpr = '(' + rightExpr + ')';
		}

		return leftExpr + ' ' + this.getOperator() + ' ' + rightExpr;
	}
};