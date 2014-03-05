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
 * Select class for building select statements
 *
 * @param {Array.<String|AbstractExpr>} args
 * @constructor
 */
function Select(args)
{
	this.allowedClasses = ['Func'];

	AbstractExpr.apply(this, arguments);
}
Select.prototype = new AbstractExpr();
Select.prototype.constructor = Select;

/**
 * {@inheritdoc}
 */
Select.prototype.preSeparator = '';

/**
 * {@inheritdoc}
 */
Select.prototype.postSeparator = '';





module.exports = Select;