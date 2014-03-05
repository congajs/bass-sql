/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * The QueryResult Class
 * @type {QueryResult}
 */
var QueryResult = require('../node_modules/bass/lib/query-result');

/**
 * SQL Client
 *
 * @param {Connection} db
 * @param {Logger} logger
 * @constructor
 */
function Client(db, logger)
{
	this.db = db;
	this.logger = logger;
}

module.exports = Client;

Client.prototype = {

	/**
	 * The last SQL statement to get generated
	 *
	 * @protected
	 * @type {String}
	 */
	_lastSql : null ,

	/**
	 * The SQL Params used for the last SQL statement that was generated
	 *
	 * @protected
	 * @type {Object}
	 */
	_lastSqlParams : null ,

	/**
	 * Get the last SQL statement that was generated
	 *
	 * @returns {String|null}
	 */
	getLastSql : function()
	{
		return this._lastSql;
	} ,

	/**
	 * Get the params used for the last SQL statement that was generated
	 *
	 * @returns {Object|null}
	 */
	getLastSqlParams : function()
	{
		return this._lastSqlParams;
	} ,

	/**
	 * Get the last query type that was executed (SELECT, UPDATE, DELETE, INSERT)
	 *
	 * @returns {String|null}
	 */
	getLastQueryType : function()
	{
		if (!this._lastSql)
		{
			return null;
		}
		var idx = this._lastSql.indexOf(' ');
		if (idx > -1)
		{
			return this._lastSql.substr(0, idx).replace(/^\s*|\s*$/g, '').toUpperCase();
		}
		return null;
	} ,

	/**
	 * Execute a raw query
	 * @param {String} sql
	 * @param {Object} params
	 * @param {Function} cb
	 */
	rawQuery : function(sql, params, cb)
	{
		this.logDebug(sql, params);

		this._lastSql = sql;
		this._lastSqlParams = params;

		this.db.connection.query(sql, params, function(err, result)
		{
			if (err)
			{
				console.log(err);
				cb(err, null);
			}
			else
			{
				cb(err, result);
			}
		});
	} ,

	/**
	 * Insert a new document
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert: function(metadata, collection, data, cb)
	{
		var sql = 'INSERT INTO ' + collection + ' SET ?';

		this.rawQuery(sql, data, function(err, result)
		{
			if (err)
			{
				cb(err, null);
			}
			else
			{
				data[metadata.getIdFieldName()] = result.lastInsertId;
				cb(err, data);
			}
		});
	},

	/**
	 * Update a document
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	update: function(metadata, collection, id, data, cb)
	{
		// need to remove the id from the update data
		delete data[metadata.getIdFieldName()];

		var sql = "UPDATE " + collection + " SET ";
		var numProps = Object.keys(data).length;
		var x = 0;
		var params = [];

		for (var i in data)
		{
			params.push(data[i]);
			sql += " " + i + " = ?";
			if (x < numProps-1)
			{
				sql += ", ";
			}
			x++;
		}

		sql += " WHERE " + metadata.getIdFieldName() + " = ?";
		params.push(id);

		this.rawQuery(sql, params, function(err, result)
		{
			cb(err, result);
		});
	},

	/**
	 * Remove a document by id
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove: function(metadata, collection, id, cb)
	{
		var sql = "DELETE FROM " + collection + " WHERE " + metadata.getIdFieldName() + " = ?";
		var params = [id];

		this.rawQuery(sql, params, function(err, result)
		{
			cb(err, result);
		});
	},

	/**
	 * Find a document by id
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find: function(metadata, collection, id, cb)
	{
		var fields = [];

		metadata.fields.forEach(function(field)
		{
			if (!field.table || field.table === collection)
			{
				fields.push(field.name);
			}
		});

		var sql = "SELECT " + fields.join(",") +
			" FROM " + collection + " WHERE " +
			metadata.getIdFieldName() + " = ?";

		this.rawQuery(sql, [id], function(err, result)
		{
			if (err)
			{
				cb(err, null);
			}
			else
			{
				var rows = result.rows;

				if (rows.length === 0)
				{
					result = null;
				}
				else
				{
					result = rows[0];
				}

				cb(err, result);
			}
		});
	},

	/**
	 * Find documents based on a Query
	 *
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery: function(metadata, collection, query, cb)
	{
		var self = this;
		this.findBy(metadata, collection, query.getConditions(), query._sort, query.getSkip(), query.getLimit(), function(err, result)
		{
			var queryResult = new QueryResult(query, result);
			if (query.getCountFoundRows())
			{
				self.selectFoundRows(function(err, numRows)
				{
					queryResult.totalRows = numRows;
					cb(err, queryResult);
				});
			}
			else
			{
				cb(err, queryResult);
			}
		});
	},

	/**
	 * Get a document count based on a Query
	 *
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery: function(metadata, collection, query, cb)
	{
		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll)
		{
			cursor = coll.find(mongoCriteria).count(function(err, count)
			{
				cb(err, count);
			});
		});
	},

	/**
	 * Find documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Object}    sort
	 * @param  {Number}    skip
	 * @param  {Number}    limit
	 * @param  {Function}  cb
	 * @return {void}
	 */
	findBy: function(metadata, collection, criteria, sort, skip, limit, cb)
	{
		var fields = [];

		metadata.fields.forEach(function(field)
		{
			if (!field.table || field.table === collection)
			{
				fields.push(field.name);
			}
		});

		var sql = "SELECT " + fields.join(", ") +
			" FROM " + collection;

		var params = [];

		if (Object.keys(criteria).length > 0)
		{
			sql += " WHERE ";

			var parts = [];

			for (var i in criteria)
			{
				parts.push(i + " = ?");
				params.push(criteria[i]);
			}

			sql += parts.join(' AND ');
		}

		if (typeof sort !== 'undefined' && sort !== null)
		{
			var sorts = [];

			for (var i in sort)
			{
				if (sort[i] === -1){
					sorts.push(i + ' DESC ');
				}
				else
				{
					sorts.push(i + ' ASC ');
				}
			}

			if (sorts.length > 0)
			{
				sql += " ORDER BY " + sorts.join(', ');
			}
		}

		if (skip || limit)
		{
			if (limit)
			{
				sql += ' LIMIT ' + parseInt(limit);
			}

			if (skip && limit)
			{
				sql += ' OFFSET ' + parseInt(skip);
			}
		}

		this.rawQuery(sql, params, function(err, result)
		{
			if (err)
			{
				cb(err);
			}
			else
			{
				var rows = result.rows;

				if (rows.length === 0)
				{
					rows = null;
				}

				cb(null, rows);
			}
		});
	},

	/**
	 * Create a collection
	 *
	 * @param  {[type]}   metadata   [description]
	 * @param  {[type]}   collection [description]
	 * @param  {Function} cb         [description]
	 * @return {void}
	 */
	create: function(metadata, collection, cb)
	{
		this.db.createCollection(collection, cb);
	},

	/**
	 * Drop a collection
	 *
	 * @param {Metadata} metadata
	 * @param {String}   collection
	 * @param {Function} cb
	 * @return {void}
	 */
	drop: function(metadata, collection, cb)
	{
		this.db.collection(collection, function(err, coll)
		{
			coll.drop(cb);
		});
	},

	/**
	 * Rename a collection
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename: function(metadata, collection, newName, cb)
	{
		this.db.collection(collection, function(err, coll)
		{
			coll.rename(newName, cb);
		});
	},

	/**
	 * Get a list of all of the collection names in the current database
	 *
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections: function(cb)
	{
		this.db.collections(cb);
	},

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 *
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria: function(query)
	{
		var newQuery = {};
		var conditions = query.getConditions();
		for (var field in conditions)
		{
			if (typeof conditions[field] === 'object')
			{
				var tmp = {};
				for (var i in conditions[field])
				{
					tmp['$' + i] = conditions[field][i];
				}
				newQuery[field] = tmp;
			}
			else
			{
				newQuery[field] = conditions[field];
			}
		}
		return newQuery;
	} ,

	/**
	 * Fetch the total rows from the last query
	 *
	 * @param {Function} cb
	 * @returns {void}
	 */
	selectFoundRows: function(cb)
	{
		if (this.getLastQueryType() === 'SELECT')
		{
			var sql = 'SELECT count(*) as numRows FROM ' +
					this.getLastSql()
						.replace(/^\s*select.*?from|limit \d+\s*,?\s*\d*|offset \d+/ig, ' ')
						.replace(/^\s*|\s*$/g, '');

			this.rawQuery(sql, this.getLastSqlParams(), function(err, result)
			{
				var numRows = (result && result.rows && result.rows.length > 0) ? result.rows[0].numRows : 0;
				cb(err, numRows);
			});
		}
		else
		{
			cb(new Error('selectFoundRows must be called after a SELECT Query'), null);
		}
	} ,

	/**
	 * Log a debug message
	 * @param {String} sql
	 * @param {Object} params
	 */
	logDebug : function(sql, params)
	{
		if (this.logger)
		{
			this.logger.debug('Query: ' + sql);
			this.logger.debug(params);
		}
	} ,

	/**
	 * Log information
	 * @param {String} sql
	 * @param {Object} params
	 */
	logInfo : function(sql, params)
	{
		if (this.logger)
		{
			this.logger.info('Query: ' + sql);
			this.logger.info(params);
		}
	} ,

	/**
	 * Log an error message
	 * @param {String} sql
	 * @param {Object} params
	 */
	logError : function(sql, params)
	{
		if (this.logger)
		{
			this.logger.error('Query: ' + sql);
			this.logger.error(params);
		}
	}
};
