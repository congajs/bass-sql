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
const QueryResult = require('bass').QueryResult;

// third party modules
const anyDbTransaction = require('any-db-transaction');

/**
 * SQL Client
 */
class Client {
    /**
	 *
     * @param {Connection} db
     * @param {Logger} logger
     */
	constructor(db, logger) {
		this.db = db;
		this.logger = logger;

        /**
         * The last SQL statement to get generated
         *
         * @protected
         * @type {String}
         */
        this._lastSql = null;

		/**
		 * The SQL Params used for the last SQL statement that was generated
		 *
		 * @protected
		 * @type {Object}
		 */
		this._lastSqlParams = null;

		/**
		 * The any-db-transaction object if any exists
		 *
		 * @protected
		 * @type {Transaction}
		 */
		this._transaction = null;
	}


	/**
	 * Generate a WHERE clause from a criteria object
	 * @param {Object} criteria The criteria object to generate SQL from
	 * @param {String|undefined|null|false} prefix The prefix to use before the SQL statement (if undefined WHERE, if null or false empty)
	 * @returns {null|{sql: string, params: Array}}
	 */
	generateWhereFromCriteria(criteria, prefix) {
		if (criteria instanceof Object && Object.keys(criteria).length > 0) {

			let i,
				m,
				params = [],
				parts = [],
				sql = '';

			for (i in criteria) {
				if (criteria[i] instanceof Object) {
					for (m in criteria[i]) {
						switch (m) {
							default :
							case '$eq' :
							case '$equals' :
								parts.push(i + ' = ?');
								params.push(criteria[i][m]);
								break;

							case '$gt' :
								parts.push(i + ' > ?');
								params.push(criteria[i][m]);
								break;

							case '$gte' :
								parts.push(i + ' >= ?');
								params.push(criteria[i][m]);
								break;

							case '$lt' :
								parts.push(i + ' < ?');
								params.push(criteria[i][m]);
								break;

							case '$lte' :
								parts.push(i + ' <= ?');
								params.push(criteria[i][m]);
								break;

							case '$ne' :
								parts.push(i + ' != ?');
								params.push(criteria[i][m]);
								break;

							case '$in' :
								if (Array.isArray(criteria[i][m])) {
									parts.push(i + ' in (' + (new Array(criteria[i][m].length + 1)).join('?,').replace(/,$/g, '') + ')');
									params = params.concat(criteria[i][m]);
								}
								break;

							case '$nin' :
								if (Array.isArray(criteria[i][m])) {
									parts.push(i + ' not in (' + (new Array(criteria[i][m].length + 1)).join('?,').replace(/,$/g, '') + ')');
									params = params.concat(criteria[i][m]);
								}
								break;

							case '$regex' :
								var val;
								if (criteria[i][m] instanceof RegExp ||
									criteria[i][m].constructor.name === 'RegExp') {

									val = criteria[i][m].source;

								} else {

									val = (criteria[i][m] + '').replace(/^\/|\/$/g, '');

								}

								if (val.charAt(0) !== '^') {
									val = '%' + val;
								} else {
									val = val.substr(1);
								}

								if (val.charAt(val.length - 1) !== '$') {
									val += '%';
								} else {
									val = val.substr(0, val.length - 1);
								}

								parts.push(i + ' like ?');
								params.push(val);
								break;
						}
					}
				} else {
					parts.push(i + " = ?");
					params.push(criteria[i]);
				}
			}

			sql += parts.join(' AND ');

			if (sql.length !== 0) {
				if (typeof prefix === 'undefined') {
					sql = ' WHERE ' + sql;
				} else if (typeof prefix === 'string') {
					sql = ' ' + prefix + ' ' + sql;
				}
			}

			return {sql, params};

		}
		return null;
	}

	/**
	 * Get the last SQL statement that was generated
	 *
	 * @returns {String|null}
	 */
	getLastSql() {
		return this._lastSql;
	}

	/**
	 * Get the params used for the last SQL statement that was generated
	 *
	 * @returns {Object|null}
	 */
	getLastSqlParams() {
		return this._lastSqlParams;
	}

	/**
	 * Get the last query type that was executed (SELECT, UPDATE, DELETE, INSERT)
	 *
	 * @returns {String|null}
	 */
	getLastQueryType() {
		if (!this._lastSql) {
			return null;
		}

		const idx = this._lastSql.indexOf(' ');
		if (idx > -1) {
			return this._lastSql.substr(0, idx).replace(/^\s*|\s*$/g, '').toUpperCase();
		}

		return null;
	}

	/**
	 * Execute a raw query
	 * @param {String} sql
	 * @param {Object} params
	 * @param {Function} cb
	 */
	rawQuery(sql, params, cb) {
		this.logDebug(sql, params);

		this._lastSql = sql;
		this._lastSqlParams = params;

		(this._transaction || this.db.connection).query(sql, params, (err, result) => {
			if (err) {

				//console.log(sql);
				//console.log(params);
				console.error(err.stack);
				cb(err, null);

			} else {

				cb(err, result);
			}
		});
	}

	/**
	 * Create collection level locks on one or more collections
	 *
	 * @param {String|Array.<Object>} locks The collection names and lock types you want to lock
	 * @param {Function} cb
	 * @returns {void}
	 */
	createLock(locks, cb) {

		let sql = 'LOCK TABLES';

		if (!Array.isArray(locks)) {
			locks = [locks];
		}

		try {
			locks.forEach(lock => {
				if (typeof lock !== 'string' && lock instanceof Object) {
					if (typeof lock.collection === 'undefined') {
						throw new Error('Collection name not provided for table lock');
					}

					sql += ' ' + lock.collection;

					if (typeof lock.type === 'undefined') {
						lock.type = 'WRITE';
					}

					sql += ' ' + lock.type;

				} else if (typeof lock === 'string') {

					sql += ' ' + lock;

					let checkLock = lock.toUpperCase();
					if (checkLock.indexOf(' READ') === -1 &&
						checkLock.indexOf(' WRITE') === -1) {

						sql += ' WRITE';
					}

				} else {
					throw new Error('Invalid collection lock specified: ' + lock);
				}
			});
		} catch (e) {
			cb(e, null);
			return;
		}

		if (sql === 'LOCK TABLES') {
			cb(new Error('Not enough information to create a lock.'), null);
			return;
		}

		this.rawQuery(sql, null, cb);
	}

	/**
	 * Release all table locks
	 *
	 * @param {*} locks This param is ignored
	 * @param {Function} cb Callback to execute after locks release
	 * @returns {void}
	 */
	releaseLock(locks, cb) {
		this.rawQuery('UNLOCK TABLES', null, cb);
	}

	/**
	 * Start a transaction
	 *
	 * @params {Function} cb
	 * @returns {void}
	 */
	startTransaction(cb) {
		this.logDebug('START TRANSACTION');

		this._transaction = anyDbTransaction(this.db.connection, err => {
			if (err) {
				this._transaction = null;
			}
			cb(err);
		});
	}

	/**
	 * Commit a transaction / run a commit command
	 *
	 * @param {Function} cb
	 * @returns {void}
	 */
	commitTransaction(cb) {
		if (!this._transaction) {
			cb(new Error('No transaction'), null);
			return;
		}

		this.logDebug('COMMIT');

		this._transaction.commit(err => {
			this._transaction = null;
			cb(err);
		});
	}

	/**
	 * Rollback a transaction
	 *
	 * @param {Function} cb
	 * @returns {void}
	 */
	rollbackTransactionn(cb) {
		if (!this._transaction) {
			cb(new Error('No transaction'));
			return;
		}

		this.logDebug('ROLLBACK');

		this._transaction.rollback(err => {
			this._transaction = null;
			cb(err);
		});
	}

	/**
	 * Insert a new document
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert(metadata, collection, data, cb) {
		const sql = 'INSERT INTO ' + collection + ' SET ?';

		this.rawQuery(sql, data, (err, result) => {
			if (err) {

				cb(err, null);

			} else {

				const idField = metadata.getIdFieldName();
				if (idField) {
					data[idField] = result.lastInsertId;
				}

				cb(null, data);
			}
		});
	}

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
	update(metadata, collection, id, data, cb) {

		// need to remove the id from the update data
		delete data[metadata.getIdFieldName()];

		let i ,
			x = 0 ,
			params = [] ,
			sql = "UPDATE " + collection + " SET " ,
			numProps = Object.keys(data).length;

		for (i in data) {

			params.push(data[i]);
			sql += " " + i + " = ?";

			if (x < numProps-1) {
				sql += ", ";
			}

			x++;
		}

		/* UNSURE : check version? see bass-mongodb

			if (typeof data[metadata.versionProperty] !== 'undefined' && data[metadata.versionProperty]) {
				params[metadata.versionProperty] = data[metadata.versionProperty] - 1;
			}

		 */

		sql += " WHERE " + metadata.getIdFieldName() + " = ?";
		params.push(id);

		this.rawQuery(sql, params, (err, result) => {

			cb(err, result);

		});
	}

	/**
	 * Update documents by simple criteria
	 *
	 * @param  {Metadata} metadata
	 * @param  {String} collection
	 * @param  {Object} criteria
	 * @param {Object} data
	 * @param  {Function} cb
	 * @return {void}
	 */
	updateBy(metadata, collection, criteria, data, cb) {
        let where;

        if (Object.keys(criteria).length !== 0) {
            where = this.generateWhereFromCriteria(criteria);
        } else {
            where = {sql: '', params: []};
        }

		if (where) {

			// need to remove the id from the update data
			const idFieldName = metadata.getIdFieldName();
			if (typeof data[idFieldName] !== 'undefined') {
				delete data[metadata.getIdFieldName()];
			}

			let i ,
				x = 0 ,
				sql = "UPDATE " + collection + " SET " ,
				numProps = Object.keys(data).length;

			for (i in data) {

				where.params.push(data[i]);
				sql += " " + i + " = ?";

				if (x < numProps-1) {
					sql += ", ";
				}

				x++;
			}

			// NOTE : we can't keep track of the version or updatedAt with this method

			sql += where.sql;

			this.rawQuery(sql, where.params, (err, result) => {

				cb(err, result);

			});

		} else {

            // if you passed in criteria, but it wasn't mapped, fail for your safety
            cb(new Error('Invalid or empty WHERE clause.  Cannot update'));
		}
	}

	/**
	 * Remove a document by id
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove(metadata, collection, id, cb) {

		const idFieldName = metadata.getIdFieldName();

		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			const sql = "DELETE FROM " + collection + " WHERE " + idFieldName + " = ?";
			const params = [id];

			this.rawQuery(sql, params, (err, result) => {
				cb(err, result);
			});

		}
	}

	// TODO : removeOneBy (different for pgsql / oracle / mysql / mssql)

	/**
	 * Remove documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Function}  cb
	 * @return {void}
	 */
	removeBy(metadata, collection, criteria, cb) {
		let where;

		if (Object.keys(criteria).length !== 0) {
            where = this.generateWhereFromCriteria(criteria);
		} else {
            where = {sql: '', params: []};
		}

		if (where) {

			const sql = "DELETE FROM " + collection + where.sql;

			// cb(err, result)
			this.rawQuery(sql, where.params, cb);

		} else {

			// if you passed in criteria, but it wasn't mapped, fail for your safety
			cb(new Error('Invalid or empty WHERE clause.  Cannot delete'));
		}
	}

	/**
	 * Find a document by id
	 *
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find(metadata, collection, id, cb) {
		const fields = [];

		metadata.fields.forEach(function(field) {
			if (!field.table || field.table === collection) {
				fields.push(field.name);
			}
		});

		const sql = "SELECT " + fields.join(",") +
					" FROM " + collection +
					" WHERE " + metadata.getIdFieldName() + " = ?";

		this.rawQuery(sql, [id], (err, result) => {
			if (err) {

				cb(err, null);

			} else {

				const rows = result.rows;

				if (rows.length === 0) {
					result = null;
				} else {
					result = rows[0];
				}

				cb(err, result);
			}
		});
	}

	/**
	 * Find documents based on a Query
	 *
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery(metadata, collection, query, cb) {

		this.findBy(
			metadata,
			collection,
			this.convertQueryToCriteria(query),
			query._sort,
			query.getSkip(),
			query.getLimit(),
			(err, result) => {
				const queryResult = new QueryResult(query, result);
				if (query.getCountFoundRows()) {
					this.selectFoundRows((err, numRows) => {
						queryResult.totalRows = numRows;
						cb(err, queryResult);
					});
				} else {
					cb(err, queryResult);
				}
			}
		);
	}

	/**
	 * Get a document count based on a Query
	 *
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery(metadata, collection, query, cb) {

		const criteria = this.convertQueryToCriteria(query);

		let sql = "SELECT count(*) AS num FROM " + collection;

		let params;
		let where = this.generateWhereFromCriteria(criteria);

		if (where) {
			params = where.params;
			sql += where.sql;
		} else {
			params = [];
		}

		this.rawQuery(sql, params, (err, result) => {
			if (err) {
				cb(err);
			} else {
                let count = 0;

				const rows = result.rows;

				if (rows && rows.length !== 0) {
					count = parseInt(rows[0].num || 0, 10);
				}

				cb(null, count);
			}
		});

	}

	/**
	 * Find documents where a field has a value in an array of values
	 *
	 * @param {Metadata} metadata The metadata for the document (entity) type you are fetching
	 * @param {String} field The table's field to search by
	 * @param {Array.<(String|Number)>} values Array of values to search for
	 * @param {Object|null} sort Object hash of field names to sort by, -1 value means DESC, otherwise ASC
	 * @param {Number|null} limit The limit to restrict results
	 * @param {Function} cb Callback function
	 */
	findWhereIn(metadata, field, values, sort, limit, cb) {

		let sql = 'SELECT `' + metadata.collection + '`.* ';
		sql += 'FROM `' + metadata.collection + '` ';
		sql += 'WHERE `' + metadata.collection + '`.`' + field + '` IN ( ' +
				((new Array(values.length + 1)).join('?,').replace(/,*$/g, '')) + ') ';

		if (sort !== undefined && sort !== null) {
			const sorts = [];

			let i;
			for (i in sort) {
				if (sort[i] === -1){
					sorts.push('`' + metadata.collection + '`.`' + i + '` DESC ');
				} else {
					sorts.push('`' + metadata.collection + '`.`' + i + '` ASC ');
				}
			}

			if (sorts.length !== 0) {
				sql += " ORDER BY " + sorts.join(', ');
			}
		}

		if (limit) {
			limit = parseInt(limit, 10);
			if (!isNaN(limit)) {
				sql += ' LIMIT 0,' + limit + ' ';
			}
		}

		this.rawQuery(sql, values, (err, result) => {
			let rows = result.rows;
			if (rows.length === 0) {
				rows = null;
			}
			cb(err, rows);
		});
	}

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
	findBy(metadata, collection, criteria, sort, skip, limit, cb) {
		const fields = [];

		metadata.fields.forEach(field => {
			if (!field.table || field.table === collection) {
				fields.push(field.name);
			}
		});

		let sql = "SELECT " + fields.join(", ") + " FROM " + collection;

		let params = [];
		const where = this.generateWhereFromCriteria(criteria);

		if (where) {
			params = where.params;
			sql += where.sql;
		}

		if (sort !== undefined && sort !== null) {
			let i;
			const sorts = [];

			for (i in sort) {
				if (sort[i] === -1){
					sorts.push(i + ' DESC ');
				} else {
					sorts.push(i + ' ASC ');
				}
			}

			if (sorts.length !== 0) {
				sql += " ORDER BY " + sorts.join(', ');
			}
		}

		if (skip || limit) {
			if (limit) {
				sql += ' LIMIT ' + parseInt(limit, 10);

				if (skip) {
                    sql += ' OFFSET ' + parseInt(skip, 10);
				}
			}
		}

		this.rawQuery(sql, params, (err, result) => {
			if (err) {
				cb(err);
			} else {
				let rows = result.rows;
				if (rows.length === 0) {
					rows = null;
				}
				cb(null, rows);
			}
		});
	}

	/**
	 * Create a collection
	 *
	 * @param  {[type]}   metadata   [description]
	 * @param  {[type]}   collection [description]
	 * @param  {Function} cb         [description]
	 * @return {void}
	 *
     * TODO: support for create table
	 * TODO: create table needs to map bass field type to sql driver type and needs to know engine type for mysql drivers, etc.
	 *
	 *
		create(metadata, collection, cb) {
			this.rawQuery('CREATE TABLE ' + collection, (err, result) => {
				cb(err, result);
			});
		}
	 */

	/**
	 * Drop a collection
	 *
	 * @param {Metadata} metadata
	 * @param {String}   collection
	 * @param {Function} cb
	 * @return {void}
	 */
	drop(metadata, collection, cb) {
		this.rawQuery('DROP TABLE ' + collection, (err, result) => {
			cb(err, result);
		});
	}

	/**
	 * Rename a collection
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename(metadata, collection, newName, cb) {
		this.db.collection(collection, (err, coll) => {
			if (err) {
				cb(err);
				return;
			}
			coll.rename(newName, cb);
		});
	}

	/**
	 * Get a list of all of the collection names in the current database
	 *
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections(cb) {
		this.db.collections(cb);
	}

	/**
	 * Convert a Bass Query to criteria format
	 *
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria(query) {

		let field;
		const newQuery = {};
		const conditions = query.getConditions();

		for (field in conditions) {
			if (typeof conditions[field] === 'object') {
				let i;
				const tmp = {};

				for (i in conditions[field]) {
					tmp['$' + i] = conditions[field][i];
				}

				newQuery[field] = tmp;
			} else {

				newQuery[field] = conditions[field];
			}
		}

		return newQuery;
	}

	/**
	 * Fetch the total rows from the last query
	 *
	 * @param {Function} cb
	 * @returns {void}
	 */
	selectFoundRows(cb) {
		// TODO: I don't like relying on getLast* for this
		// TODO: if mysql were to implement this, it could do it using SQL_CALC_FOUND_ROWS - might be a little work

		if (this.getLastQueryType() === 'SELECT') {
			let sql = 'SELECT count(*) as numRows FROM ' +
					this.getLastSql()
						.replace(/^\s*select.*?from|limit \d+\s*,?\s*\d*|offset \d+/ig, ' ')
						.replace(/^\s*|\s*$/g, '');

			this.rawQuery(sql, this.getLastSqlParams(), (err, result) => {

				const numRows = (result && result.rows && result.rows.length > 0)
					? result.rows[0].numRows
					: 0;

				cb(err, numRows);

			});
		} else {
			cb(new Error('selectFoundRows must be called after a SELECT Query'), null);
		}
	}

	/**
	 * Log a debug message
	 * @param {String} sql
	 * @param {Object} params
	 */
	logDebug(sql, params) {
		if (this.logger) {
			this.logger.debug('Query: ' + sql);
			if (params) {
				this.logger.debug(params);
			}
		}
	}

	/**
	 * Log information
	 * @param {String} sql
	 * @param {Object} params
	 */
	logInfo(sql, params) {
		if (this.logger) {
			this.logger.info('Query: ' + sql);
			if (params) {
				this.logger.debug(params);
			}
		}
	}

	/**
	 * Log an error message
	 * @param {String} sql
	 * @param {Object} params
	 */
	logError(sql, params) {
		if (this.logger) {
			this.logger.error('Query: ' + sql);
			if (params) {
				this.logger.debug(params);
			}
		}
	}
}

module.exports = Client;