/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var AbstractExpr = require('./abstract-expr');

/**
 * Composite class for building query parts
 *
 * @param {Array.<String|AbstractExpr>} args
 * @constructor
 */
function Composite(args) {

	AbstractExpr.apply(this, arguments);
}
Composite.prototype = new AbstractExpr();
Composite.prototype.constructor = Composite;

/**
 * {@inheritdoc}
 */
Composite.prototype.toString = function() {

	if (this.count() === 1) {
		return this.parts[0].toString();
	}

	var components = [];

	var i, len = this.parts.length;
	for (i = 0; i < len; i++) {
		components.push(partToString(this.parts[i]));
	}

	return components.join(this.separator);
};



/**
 * Get a composite part as a string
 *
 * @param {*} part
 * @returns {*}
 */
function partToString(part) {

	var strPart = part.toString();

	if (part instanceof Object &&
		part.constructor.name === this.constructor.name &&
		part.count() > 1) {

		return this.preSeparator + strPart + this.postSeparator;
	}

	if (strPart.indexOf(' OR ') > -1 ||
		strPart.indexOf(' AND ') > -1) {

		return this.preSeparator + strPart + this.postSeparator;
	}

	return strPart;
}



module.exports = Composite;