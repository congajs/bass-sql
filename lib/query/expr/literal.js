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
 * Literal Class for building literal query parts
 *
 * @param {Array} args
 * @constructor
 */
function Literal(args) {

	AbstractExpr.apply(this, arguments);
}
Literal.prototype = new AbstractExpr();
Literal.prototype.constructor = Literal;

/**
 * {@inheritdoc}
 */
Literal.prototype.preSeparator = '';

/**
 * {@inheritdoc}
 */
Literal.prototype.postSeparator = '';



module.exports = Literal;