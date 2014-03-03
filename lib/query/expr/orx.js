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
 * Orx class for building query parts
 *
 * @param {Array.<String|AbstractExpr>} args
 * @constructor
 */
function Orx(args) {

	this.allowedClasses = [
		'Func' ,
		'Comparison' ,
		'Orx' ,
		'Andx'
	];

	AbstractExpr.apply(this, arguments);
}
Orx.prototype = new AbstractExpr();
Orx.prototype.constructor = Orx;

/**
 * {@inheritdoc}
 */
Orx.prototype.separator = ' OR ';



module.exports = Orx;