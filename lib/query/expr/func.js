/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Expression for generating query functions
 *
 * @param {String} name The function name
 * @param {Array} args The arguments to pass to the function
 * @constructor
 */
function Func(name, args) {
	this._name = name;

	if (Array.isArray(args)) {
		this._args = args;
	} else {
		this._args = [];
	}
}



Func.prototype = {
	/**
	 * The function name
	 *
	 * @type {String}
	 * @protected
	 */
	_name: null ,

	/**
	 * Array of arguments to pass to the function
	 *
	 * @type {Array}
	 * @protected
	 */
	_args: null ,

	/**
	 * Get the function name
	 *
	 * @returns {String}
	 */
	getName: function() {
		return this._name;
	} ,

	/**
	 * Get the arguments
	 *
	 * @returns {Array}
	 */
	getArguments: function() {
		return this._args;
	} ,

	/**
	 * Get the string representation of this expression
	 *
	 * @returns {String}
	 */
	toString: function() {
		return this.getName() + '(' + this.getArguments().join(', ') + ')';
	}
};



Func.prototype.constructor = Func;

module.exports = Func;