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
 * Andx class for building query parts
 *
 * @param {Array.<String|AbstractExpr>} args
 * @constructor
 */
function Andx(args) {

	this.allowedClasses = [
		'Func' ,
		'Comparison' ,
		'Orx' ,
		'Andx'
	];

	AbstractExpr.apply(this, arguments);
}
Andx.prototype = new AbstractExpr();
Andx.prototype.constructor = Andx;

/**
 * {@inheritdoc}
 */
Andx.prototype.separator = ' AND ';



module.exports = Andx;