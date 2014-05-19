/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
var Q = require('q');

// local modules
var Bass = require('bass');
var Query = Bass.Query;
var QueryResult = Bass.QueryResult;

/**
 * QueryClient for executable queries
 *
 * @param {Manager|undefined} manager The Bass Manager instance
 * @param {Query|undefined} query
 * @constructor
 */
function QueryClient(manager, query) {
	this._manager = manager;
	this._query = query || new Query();
}



QueryClient.prototype = {
	/**
	 * The Bass Manager
	 *
	 * @type {Manager}
	 */
	_manager: null ,

	/**
	 * The Bass Query object
	 *
	 * @type {Query}
	 */
	_query: null ,

	/**
	 * The query parameters
	 *
	 * @type {Array|Object|*}
	 */
	_params: null ,

	/**
	 * The raw SQL to use
	 *
	 * @type {String}
	 */
	_sql: null ,

	/**
	 * The known repository name
	 *
	 * @type {String|null}
	 */
	_repositoryName: null ,

	/**
	 * Boolean flag to include raw data
	 *
	 * @var {Boolean}
	 */
	_includeRawData: false ,

	/**
	 * Boolean flag to map the response data or not
	 *
	 * @var {Boolean}
	 */
	_mapData: true ,

	/**
	 * Set the include-raw-data flag
	 *
	 * @returns {Boolean}
	 */
	setIncludeRawData: function(bool) {
		this._includeRawData = !!bool;
		return this;
	} ,

	/**
	 * Get the include-raw-data flag
	 *
	 * @returns {Boolean}
	 */
	getIncludeRawData: function() {
		return !!this._includeRawData;
	} ,

	/**
	 * Set the repository name
	 *
	 * @param {String} name
	 * @returns {QueryClient}
	 */
	setRepositoryName: function(name) {
		this._repositoryName = name;
		return this;
	} ,

	/**
	 * Get the repository name
	 *
	 * @returns {String|null}
	 */
	getRepositoryName: function() {
		return this._repositoryName;
	} ,

	/**
	 * Get the manager
	 * @returns {Manager}
	 */
	getManager: function() {
		return this._manager;
	} ,

	/**
	 * Set the query parameters
	 *
	 * @param {Object} params
	 * @returns {QueryClient}
	 */
	setParameters: function(params) {
		this._params = params;
		return this;
	} ,

	/**
	 * Get the parameters
	 *
	 * @returns {Object}
	 */
	getParameters: function() {
		return this._params;
	} ,

	/**
	 * Set the number of maximum result that can be returned
	 *
	 * @param {Number} max
	 * @returns {QueryClient}
	 */
	setMaxResults: function(max) {
		this._query.limit(max);
		return this;
	} ,

	/**
	 * Get the number of maximum result that can be returned
	 *
	 * @returns {Number}
	 */
	getMaxResults: function() {
		return this._query.getLimit();
	} ,

	/**
	 * Set the Query offset
	 *
	 * @param {Number} offset
	 * @returns {QueryClient}
	 */
	setOffset: function(offset) {
		this._query.skip(offset);
		return this;
	} ,

	/**
	 * Get the Query offset
	 *
	 * @returns {Number}
	 */
	getOffset: function() {
		return this._query.getSkip();
	} ,

	/**
	 * Set the count-found-rows boolean flag
	 *
	 * @param {Boolean} bool
	 * @returns {QueryClient}
	 */
	setCountFoundRows: function(bool) {
		this._query.countFoundRows(!!bool);
		return this;
	} ,

	/**
	 * Get the count-found-rows boolean flag
	 *
	 * @returns {Boolean}
	 */
	getCountFoundRows: function() {
		return !!this._query.getCountFoundRows();
	} ,

	/**
	 * Get the map-data boolean flag
	 *
	 * @param {Boolean} mapData
	 * @returns {QueryClient}
	 */
	setMapData: function(mapData) {
		this._mapData = !!mapData;
		return this;
	} ,

	/**
	 * Set the map-data boolean flag
	 *
	 * @returns {Boolean}
	 */
	getMapData: function() {
		return !!this._mapData;
	} ,

	/**
	 * Set the raw SQL to use
	 *
	 * @param {String} sql
	 * @returns {QueryClient}
	 */
	setSql: function(sql) {
		this._sql = sql;
		return this;
	} ,

	/**
	 * Get the raw SQL to use
	 *
	 * @returns {String}
	 */
	getSql: function() {
		if (!this._sql) {
			return null;
		}

		var sql = this._sql;

		var limit = this.getMaxResults();
		if (limit) {
			sql += ' LIMIT ' + limit;
		}

		var offset = this.getOffset();
		if (offset) {
			sql += ' OFFSET ' + offset;
		}

		return sql;
	} ,

	/**
	 * Set the Query object to use for skip, limit, and additional conditions
	 *
	 * @param {Query} query
	 * @returns {QueryClient}
	 */
	setQuery: function(query) {
		if (query) {
			this._query = query;
		}
		return this;
	} ,

	/**
	 * Get the Query object to use for skip, limit, and additional conditions
	 *
	 * @returns {Query}
	 */
	getQuery: function() {
		return this._query;
	} ,

	/**
	 * Prepare SQL and parameters for execution
	 *
	 * @param {String} sql
	 * @param {Object|Array} params
	 * @returns {Object}
	 * @throws Error
	 * @private
	 */
	_prepareSql: function(sql, params) {

		var match,
			paramName,
			paramCount = 0;

		for (paramName in params) {
			paramCount += 1;
		}

		if (Array.isArray(params)) {
			match = sql.match(/\?/g) || [];
			if (match.length !== paramCount) {
				throw new Error('Parameter count does not match');
			}
			return {sql: sql, params: params};
		}

		var _sql = sql;
		var _params = [];
		var unique = [];
		var loopReg = /(\s*|,|\():[\.a-z_]+(\s*|,|\)|[\.a-z\_]+)/i;

		while (match = _sql.match(loopReg)) {

			paramName = match[0].replace(/^\s*|\s*$/g, '').replace(/^:/, '');
			if (typeof params[paramName] !== 'undefined') {
				_params.push(params[paramName]);
			} else {
				throw new Error('Parameter not found, "' + paramName + '"');
			}

			if (unique.indexOf(paramName) === -1) {
				unique.push(paramName);
			}

			_sql = _sql.replace(':' + paramName, '?');
		}

		if (unique.length !== paramCount) {
			throw new Error('Parameter count does not match');
		}

		return {
			sql: _sql ,
			params: _params
		};
	} ,

	/**
	 * Get the query result
	 *
	 * @param {String|undefined} repositoryName The repository name (context) to run this query under (optional with SQL, required without)
	 * @returns {Promise}
	 * @throws Error
	 */
	getResult: function(repositoryName) {

		if (!repositoryName && this._repositoryName) {
			repositoryName = this._repositoryName;
		}

		var sql = this.getSql();
		if (sql) {
			var query = this._prepareSql(sql, this.getParameters());

			var deferred = Q.defer();

			var self = this;
			this._manager.client.rawQuery(query.sql, query.params, function(err, result) {

				if (err) {
					deferred.reject(err);

				} else {
					if (result === null){
						deferred.resolve(null);

					} else {

						var data = (typeof result['rows'] !== 'undefined') ? result['rows'] : null;

						var queryResult = new QueryResult(self._query, data);

						if (self.getIncludeRawData()) {
							queryResult.rawData = result;
						}

						if (repositoryName) {
							var repo = self.getManager().getRepository(repositoryName);

							if (!repo) {

								deferred.reject(new Error('Unable to find repository: ' + repositoryName));

							} else {

								var finish = function(queryResult) {
									if (self.getCountFoundRows()) {
										self._manager.client.selectFoundRows(function(err, numRows)
										{
											if (err) {
												console.log(err.stack);
												deferred.reject(err);
												return;
											}

											queryResult.totalRows = numRows;
											deferred.resolve(queryResult);
										});
									} else {
										deferred.resolve(queryResult);
									}
								};

								if (self.getMapData()) {

									// hydrate the results if we are allowed to map the data
									self.getManager().mapDataToModels(
										// document's metadata we are mapping
										repo.metadata,

										// the data we are mapping
										data,

										// final callback
										function(err, documents) {
											queryResult.setData(documents);

											finish(queryResult);
										}
									);

								} else {

									// just pass through the raw results
									finish(queryResult);
								}
							}
						} else {

							// no repository, just return the results
							deferred.resolve(queryResult);
						}
					}
				}
			});

			return deferred.promise;
		}

		if (!repositoryName) {
			throw new Error('The repository name is required when the raw SQL is not set.');
		}

		return this._manager.findByQuery(repositoryName, this._query);
	}
};



QueryClient.prototype.constructor = QueryClient;

module.exports = QueryClient;