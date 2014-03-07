/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// local modules
var BaseQueryBuilder = require('../../../../query/query-builder');

/**
 * QueryBuilder for MySql
 *
 * @param {Manager} manager
 * @constructor
 */
function QueryBuilder(manager) {
	BaseQueryBuilder.apply(this, arguments);
}
QueryBuilder.prototype = new BaseQueryBuilder();
QueryBuilder.prototype.constructor = QueryBuilder;



module.exports = QueryBuilder;