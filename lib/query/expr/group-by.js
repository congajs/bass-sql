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
 * Group By Class for building group by query parts
 *
 * @param {Array} args
 * @constructor
 */
function GroupBy(args) {

	AbstractExpr.apply(this, arguments);
}
GroupBy.prototype = new AbstractExpr();
GroupBy.prototype.constructor = GroupBy;

/**
 * {@inheritdoc}
 */
GroupBy.prototype.preSeparator = '';

/**
 * {@inheritdoc}
 */
GroupBy.prototype.postSeparator = '';



module.exports = GroupBy;