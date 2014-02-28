/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * The Bass Class
 *
 * @type {Client} Class Constructor
 */
var Client = require('../../client');

/**
 * MysqlClient Class extends Client
 * Used to perform Mysql specific operations and queries
 *
 * @param {Connection} db
 * @param {Logger} logger
 * @constructor
 */
function MysqlClient(db, logger)
{
	Client.apply(this, arguments);
}
MysqlClient.prototype = new Client();
MysqlClient.prototype.constructor = MysqlClient;

/**
 * Flag to know if the current SELECT statement should use SQL_CALC_FOUND_ROWS
 * @type {Boolean}
 * @private
 */
//MysqlClient.prototype._countFoundRows = false;

/**
 * {@inheritdoc}
 *
MysqlClient.prototype.rawQuery = function(sql, params, cb)
{
	if (this._countFoundRows)
	{
		sql = sql.replace(/^\s*select\s*(.*?)\s*from/i, 'SELECT SQL_CALC_FOUND_ROWS $1 FROM');
		this._countFoundRows = false;
	}
	Client.prototype.rawQuery.apply(this, [sql, params, cb]);
};*/

/**
 * {@inheritdoc}
 *
MysqlClient.prototype.findByQuery = function(metadata, collection, query, cb)
{
	if (query.getCountFoundRows())
	{
		//this._countFoundRows = true;
	}
	Client.prototype.findByQuery.apply(this, [metadata, collection, query, cb]);
};*/

/**
 * {@inheritdoc}
 *
MysqlClient.prototype.selectFoundRows = function(cb)
{
	if (this.getLastQueryType() === 'SELECT' &&
		this.getLastSql().toUpperCase().indexOf('SQL_CALC_FOUND_ROWS') > -1)
	{
		// NOTE: this doesn't work across threads!!!!!
		this.rawQuery('select found_rows() as numRows', null, function(err, result)
		{
			console.log(result);
			var numRows = (result && result.rows && result.rows.length > 0) ? result.rows[0].numRows : 0;
			cb(err, numRows);
		});
	}
	else
	{
		cb(new Error('selectFoundRows must be called after a SELECT Query with countFoundRows'), null);
	}
};*/

module.exports = MysqlClient;