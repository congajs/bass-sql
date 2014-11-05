/*
 * This file is part of the bass-sql library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// local modules
var QueryClient = require('./query-client');

// expressions
var Select = require('./expr/select');
var From = require('./expr/from');
var Comparison = require('./expr/comparison');
var Andx = require('./expr/andx');
var Orx = require('./expr/orx');
var GroupBy = require('./expr/group-by');
var OrderBy = require('./expr/order-by');
var Join = require('./expr/join');

/**
 * The Query Builder
 *
 * @param {Manager} manager
 * @constructor
 */
function QueryBuilder(manager) {
	if (manager) {
		this._manager = manager;
		this._params = {};
		this._joinTableMap = {};
		this._parts = {
			'select' : false ,
			'from' : false ,
			'join' : false ,
			'set' : false ,
			'where' : false ,
			'groupBy' : false ,
			'having' : false ,
			'orderBy': false
		};
	}
}



QueryBuilder.STATE_CLEAN = 0;
QueryBuilder.STATE_DIRTY = 1;

QueryBuilder.TYPE_SELECT = 'SELECT';
QueryBuilder.TYPE_UPDATE = 'UPDATE';
QueryBuilder.TYPE_DELETE = 'DELETE';
QueryBuilder.TYPE_INSERT = 'INSERT';



QueryBuilder.prototype = {
	/**
	 * The state of this object, clean or dirty
	 *
	 * @type {Number}
	 * @protected
	 */
	_state: QueryBuilder.STATE_CLEAN ,

	/**
	 * The Query Type
	 *
	 * @type {String}
	 * @protected
	 */
	_queryType: null ,

	/**
	 * The Bass Manager
	 *
	 * @type {Manager}
	 * @protected
	 */
	_manager: null ,

	/**
	 * The offset for the query
	 *
	 * @type {Number}
	 * @protected
	 */
	_offset: null ,

	/**
	 * Parameters for the query
	 *
	 * @type {Array|Object}
	 * @protected
	 */
	_params: null ,

	/**
	 * Max results for the query (the "limit")
	 *
	 * @type {Number}
	 * @protected
	 */
	_maxResults: null ,

	/**
	 * Flag for a DISTINCT query
	 *
	 * @type {Boolean}
	 * @protected
	 */
	_distinct: false ,

	/**
	 * Query parts / expressions
	 *
	 * @type {Object}
	 * @protected
	 */
	_parts: null ,

	/**
	 * Object hash mapping table names to their aliases
	 *
	 * @type {Array.<String>}
	 * @protected
	 */
	_tableMap: null ,

	/**
	 * Object hash mapping joining table aliases to root aliases
	 *
	 * @type {Array.<String>}
	 * @protected
	 */
	_joinTableMap: null ,

	/**
	 * The root document names after they have been found
	 *
	 * @type {Array.<String>}
	 * @protected
	 */
	_rootDocuments: null ,

	/**
	 * The root alias names after they have been found
	 *
	 * @type {Array.<String>}
	 * @protected
	 */
	_rootAliases: null ,

	/**
	 * Cached SQL from the last time it was generated
	 *
	 * @type {String}
	 * @protected
	 */
	_sql: null ,

	/**
	 * The known repository / document name
	 *
	 * @type {String|null}
	 * @protected
	 */
	_repositoryName: null ,

	// TODO : Expr helper class and expression builder
	expr: function() {

		//return this._manager.getExpressionBuilder();
	} ,

	/**
	 * Set the repository name
	 *
	 * @param {String} name
	 * @returns {QueryBuilder}
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
	 * Get the Bass Manager
	 *
	 * @returns {Manager}
	 */
	getManager: function() {
		return this._manager;
	} ,

	/**
	 * See if this object is dirty
	 *
	 * @returns {Boolean}
	 */
	isDirty: function() {
		return this._state === QueryBuilder.STATE_DIRTY;
	} ,

	/**
	 * Mark this object as "dirty"
	 *
	 * @returns {QueryBuilder}
	 * @protected
	 */
	_dirty: function() {
		this._sql = null;
		this._tableMap = null;
		this._rootDocuments = null;
		this._rootAliases = null;

		this._state = QueryBuilder.STATE_DIRTY;

		return this;
	} ,

	/**
	 * Mark this object as "clean"
	 *
	 * @returns {QueryBuilder}
	 * @protected
	 */
	_clean: function() {
		this._state = QueryBuilder.STATE_CLEAN;
		return this;
	} ,

	/**
	 * Get relational fields for a table (through metadata mapping)
	 * @param {String} tbl The table (collection) name
	 * @returns {*}
	 */
	getTableRelationFields: function(tbl) {
		try {

			if (!tbl || tbl.length === 0) {
				tbl = this.getRootAlias();
			}

			var metadata = this._manager.registry.getMetadataByName(tbl);

			if (metadata) {
				// TODO : Bass:OneToOne / etc. doesn't work in bass-sql, it thinks all maps are mongodb documents
				return metadata.getRelationFields() || [];
			}

		} catch (e) { console.error(e.stack); }

		return [];
	} ,

	/**
	 * Parse out the table alias from an SQL fragment
	 *
	 * @param {String} val
	 * @returns {String}
	 */
	parseTableAlias: function(val) {

		var trim = /^\s*|\s*$/g;

		val = val.replace(trim, '');

		if (val.indexOf('.') > -1) {
			// the table name / alias is on the left of the dot, like in "select u.id"
			val = val.split('.').shift().replace(trim, '');
		}

		var tableMap = this.getTableMap();

		if (typeof tableMap[val] === 'undefined') {
			// the alias doesn't exist in the map
			// loop tables, maybe we have the table name
			var found = null;
			for (var t in tableMap) {
				if (tableMap[t].table === val) {
					found = true;
					val = tableMap[t].alias;
					break;
				}
			}

			// we don't know the parent alias yet, look in metadata
			if (!found) {
				var relationFields = this.getTableRelationFields();
				//console.log(relationFields);

				// TODO : finish

				if (!found) {
					return null;
				}
			}
		}

		return val;
	} ,

	/**
	 * Get a map of the tables (and their aliases) involved in this query
	 *
	 * @returns {Object}
	 */
	getTableMap: function() {

		// no parts, no table-map
		if (!this._parts.from) {
			return null;
		}

		// use cache when possible
		if (this._tableMap) {
			// NOTE: when this object's state becomes DIRTY, this variable should be reset
			return this._tableMap;
		}

		var spaceIdx ,
			fromTable ,
			fromClause ,
			alias ,
			trim = /^\s*|\s*$/g ,
			joinTables,
			tables = {};

		if (this._parts.from.constructor.name === 'From') {

			fromClause = this._parts.from;

			// find table aliases that join with the current table
			joinTables = [];
			for (alias in this._joinTableMap) {
				if (this._joinTableMap[alias] === fromClause.getAlias()) {
					joinTables.push(alias);
				}
			}

			tables[fromClause.getAlias()] = {
				table: fromClause.getFrom() ,
				alias: fromClause.getAlias() ,
				expr: fromClause ,
				joinTables: joinTables 		// table aliases that join with this table
			};

		} else if (Array.isArray(this._parts.from)) {

			var i, len = this._parts.from.length;
			for (i = 0; i < len; i++) {

				fromClause = this._parts.from[i];

				if (fromClause instanceof String || typeof fromClause === 'string') {

					// the entire clause (something like, "my_table tbl")
					fromClause = fromClause.replace(trim, '').replace(/\s+as\s+/i, ' ');

					spaceIdx = fromClause.indexOf(' ');
					if (spaceIdx > 0) {

						// value up to the first space (like, "my_table")
						fromTable = fromClause.substr(spaceIdx);

						// value after the first space (like, "tbl")
						alias = fromClause
							.substr(spaceIdx + 1, fromClause.length - spaceIdx + 1)
							.replace(trim, '')
							.replace(/`/g, '');

					} else {

						fromTable = fromClause;
						alias = fromClause;

					}

					fromClause = new From(fromTable, alias);
					this._parts.from[i] = fromClause;
				}

				// find table aliases that join with the current table
				joinTables = [];
				for (alias in this._joinTableMap) {
					if (this._joinTableMap[alias] === fromClause.getAlias()) {
						joinTables.push(alias);
					}
				}

				tables[fromClause.getAlias()] = {
					table: fromClause.getFrom() ,
					alias: fromClause.getAlias() ,
					expr: fromClause ,
					joinTables: joinTables 		// table aliases that join with this table
				};
			}
		}

		// cache the map
		this._tableMap = tables;

		return tables;
	} ,

	/**
	 * Get the root alias (table name)
	 *
	 * @returns {*}
	 */
	getRootAlias: function() {
		var aliases = this.getRootAliases();

		if (aliases.length === 0) {
			return null;
		}

		return aliases[0];
	} ,

	/**
	 * Get the root aliases (tables involved on building the query, from / join clauses)
	 *
	 * @returns {Array.<String>}
	 */
	getRootAliases: function() {
		if (this._rootAliases && !this.isDirty()) {
			return this._rootAliases;
		}

		var aliases = [];

		var tableMap = this.getTableMap();
		for (var t in tableMap) {
			aliases.push(tableMap[t].alias);
		}

		this._rootAliases = aliases;

		return aliases;
	} ,

	/**
	 * Find the alias of the table that joins with the provided table alias
	 *
	 * @param {String} alias The joining table alias
	 * @param {String} parentAlias The root alias that this joining table joins with
	 * @returns {String|null}
	 */
	findJoiningAlias: function(alias, parentAlias) {
		var found = null;
		if (this.getRootAliases().indexOf(parentAlias) > -1) {

			found = parentAlias;

		} else if (typeof this._joinTableMap[parentAlias] !== 'undefined') {

			found = this._joinTableMap[parentAlias];

		}
		if (found) {
			this._joinTableMap[alias] = found;
		}
		return found;
	} ,

	/**
	 * Get the root collection (table name)
	 *
	 * @returns {String|null}
	 */
	getRootCollection: function() {
		var alias = this.getRootAlias();
		var tableMap = this.getTableMap();
		if (typeof tableMap[alias] !== 'undefined') {
			return tableMap[alias].table;
		}
		return null;
	} ,

	/**
	 * Get the root document name
	 *
	 * @returns {String|null}
	 */
	getRootDocument: function() {
		var documents = this.getRootDocuments();

		if (documents.length === 0) {
			return null;
		}

		return documents[0];
	} ,

	/**
	 * Get the root document (model) names - documents that match the root tables
	 *
	 * @returns {Array.<String>}
	 */
	getRootDocuments: function() {
		if (this._rootDocuments && !this.isDirty()) {
			return this._rootDocuments;
		}

		var documents = [];

		var tableMap = this.getTableMap();
		for (var t in tableMap) {

			var documentName = this._manager
				.mapper
				.mapCollectionNameToDocumentName(tableMap[t].table);

			if (documentName) {
				documents.push(documentName);
			}

		}

		this._rootDocuments = documents;

		return documents;
	} ,

	/**
	 * Get the SQL from this QueryBuilder
	 *
	 * @returns {String}
	 * @throws Error
	 */
	getSql: function() {
		if (!this.isDirty() && this._sql) {
			return this._sql;
		}

		var sql;

		switch (this._queryType) {
			case QueryBuilder.TYPE_DELETE :
			{
				sql = this._getSqlForDelete();
				break;
			}
			case QueryBuilder.TYPE_UPDATE :
			{
				sql = this._getSqlForUpdate();
				break;
			}
			case QueryBuilder.TYPE_SELECT :
			default :
			{
				sql = this._getSqlForSelect();
				break;
			}
		}

		this._clean();
		this._sql = sql;

		return sql;
	} ,

	/**
	 * Get the SQL for a SELECT query
	 *
	 * @returns {String}
	 * @protected
	 */
	_getSqlForSelect: function() {
		var sql = 'SELECT';

		if (this.isDistinct()) {
			sql += ' DISTINCT';
		}

		return (
			sql +
			this._getSqlForQueryPart('select', {pre: ' ', separator: ', '}) +
			this._getSqlForQueryPart('from', {pre: ' FROM ', separator: ', '}) +
			this._getSqlForQueryPart('join', {pre: ' ', separator: ' '}) +
			this._getSqlForQueryPart('where', {pre: ' WHERE '}) +
			this._getSqlForQueryPart('groupBy', {pre: ' GROUP BY ', separator: ', '}) +
			this._getSqlForQueryPart('having', {pre: ' HAVING '}) +
			this._getSqlForQueryPart('orderBy', {pre: ' ORDER BY ', separator: ', '})
		).replace(/^\s*|\s*$/g, '');
	} ,

	/**
	 * Get the SQL for an UPDATE query
	 *
	 * @returns {String}
	 * @throws Error
	 * @protected
	 */
	_getSqlForUpdate: function() {
		var trim = /^\s*|\s*$/g;

		var fromClause = this._getSqlForQueryPart('from', {pre: ' ', separator: ', '});
		if (fromClause.replace(trim, '').length === 0) {
			throw new Error('Cannot update without a FROM clause');
		}

		var setClause = this._getSqlForQueryPart('set', {pre: ' SET ', separator: ', '});
		if (setClause.replace(trim, '').length === 0) {
			throw new Error('Cannot update without a SET clause');
		}

		var whereClause = this._getSqlForQueryPart('where', {pre: ' WHERE '});
		if (whereClause.replace(trim, '').length === 0) {
			throw new Error('Cannot update without a WHERE clause');
		}

		var joinClause = this._getSqlForQueryPart('join', {pre: ' ', separator: ' '});

		var orderByClause = this._getSqlForQueryPart('orderBy', {pre: ' ORDER BY ', separator: ', '});

		return ('UPDATE' + fromClause + joinClause + setClause + whereClause + orderByClause).replace(/^\s*|\s*$/g, '');
	} ,

	/**
	 * Get the SQL for a DELETE query
	 *
	 * @returns {String}
	 * @throws Error
	 * @protected
	 */
	_getSqlForDelete: function() {
		var trim = /^\s*|\s*$/g;

		var from = this._getSqlForQueryPart('from', {pre: ' ', separator: ', '});
		if (from.replace(trim, '').length === 0) {
			throw new Error('Cannot delete without a FROM clause');
		}

		var where = this._getSqlForQueryPart('where', {pre: ' WHERE '});
		if (where.replace(trim, '').length === 0) {
			throw new Error('Cannot delete without a WHERE clause');
		}

		var joins = this._getSqlForQueryPart('join', {pre: ' ', separator: ' '});

		var orderBy = this._getSqlForQueryPart('orderBy', {pre: ' ORDER BY ', separator: ', '});

		// TODO : need to make sure this works as expected - also check UPDATE - should behave the same
		// DELETE table FROM table INNER JOIN table2 on table2.id = table.rel_id WHERE table2.id = 5
		return ('DELETE' + from + ' FROM ' + from + joins + where + orderBy).replace(/^\s*|\s*$/g, '');
	} ,

	/**
	 * Get the SQL for a query part / parts
	 *
	 * @param {String} queryPartName The part name, SELECT, UPDATE, etc.
	 * @param {Object} options For pre, post, separator and an empty value
	 * @returns {*}
	 * @protected
	 */
	_getSqlForQueryPart: function(queryPartName, options) {
		if (typeof this._parts[queryPartName] === 'undefined') {
			return null;
		}

		if (!(options instanceof Object)) {
			options = {};
		}

		var queryPart = this._parts[queryPartName];

		if (!queryPart || (Array.isArray(queryPart) && queryPart.length === 0)) {
			return options.empty || '';
		}

		var sql = options.pre;

		if (Array.isArray(queryPart)) {
			sql += queryPart.join(options.separator || '');
		} else {
			sql += queryPart.toString();
		}

		sql += options.post || '';

		return sql;
	} ,

	/**
	 * Return a Query object
	 *
	 * @param {Query|undefined} query Optional Query object to pass to the QueryClient
	 * @returns {QueryClient}
	 */
	getQuery: function(query) {
		if (query) {
			this.importQuery(query);
		}
		return (new QueryClient(this._manager, query))
			.setRepositoryName(this._repositoryName || null)
			.setSql(this.getSql() || null)
			.setOffset(this.getOffset() || null)
			.setMaxResults(this.getMaxResults() || null)
			.setParameters(this.getParameters());
	} ,

	/**
	 * Import data from a bass Query object
	 *
	 * @param {Query} query
	 * @returns {QueryBuilder}
	 */
	importQuery: function(query) {
		if (!query) {
			return this;
		}

		if (typeof query.getConditions === 'function') {
			var x;
			var data;
			var where;
			var fieldIdx = 0;
			var conditions = query.getConditions();
			for (var field in conditions) {
				var cond = conditions[field];
				if (cond instanceof Object) {
					for (var operator in cond) {
						var value = cond[operator];
						if (value === null) {
							continue;
						}
						switch (operator) {
							default :
							case 'eq' :
							case 'equals' :
								this.andWhere(field + ' = :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'ne' :
								this.andWhere(field + ' != :field_ ' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'gt' :
								this.andWhere(field + ' > :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'gte' :
								this.andWhere(field + ' >= :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'lt' :
								this.andWhere(field + ' < :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'lte' :
								this.andWhere(field + ' <= :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'regex' :
								if (cond[operator] instanceof RegExp) {
									value = cond[operator].source;
								} else {
									value = cond[operator];
								}
								value = value.replace(/^\/|\/[a-z]*$/g, '');
								if (value.charAt(0) !== '^') {
									value = '%' + value;
								}
								if (value.charAt(value.length - 1) !== '$') {
									value += '%';
								}
								this.andWhere(field + ' like :field_' + fieldIdx);
								this.setParameter('field_' + fieldIdx, value);
								break;

							case 'in' :
								data = value;
								if (!Array.isArray(data)) {
									data = (data + '').split(',');
								}
								where = '';
								for (x = 0; x < data.length; x++) {
									this.setParameter('field_' + fieldIdx, data[x]);
									where += (x > 0 ? ',' : '') + ':field_' + (fieldIdx++);
								}
								this.andWhere(field + ' in (' + where + ')');
								value = null;
								break;

							case 'nin' :
								data = value;
								if (!Array.isArray(data)) {
									data = (data + '').split(',');
								}
								where = '';
								for (x = 0; x < data.length; x++) {
									this.setParameter('field_' + fieldIdx, data[x]);
									where += (x > 0 ? ',' : '') + ':field_' + (fieldIdx++);
								}
								this.andWhere(field + ' not in (' + where + ')');
								value = null;
								break;
						}
					}
				} else {
					if (cond === null) {
						this.andWhere(field + ' is null');
					} else {
						this.andWhere(field + ' = :field_' + fieldIdx);
						this.setParameter('field_' + fieldIdx, cond);
					}
				}
				fieldIdx += 1;
			}
		}

		if (typeof query.getSort === 'function') {
			var sort = query.getSort();
			for (var sortKey in sort) {

				var sortDir = sort[sortKey] === -1 || sort[sortKey] === '-1' ? 'DESC' : 'ASC';
				this.addOrderBy(sortKey, sortDir);
			}
		}

		if (typeof query.getLimit === 'function') {
			var limit = query.getLimit();
			if (limit && limit > 0) {
				this.setMaxResults(limit);
			}
		}

		if (typeof query.getSkip === 'function') {
			var skip = query.getSkip();
			if (skip && skip > 0) {
				this.setOffset(skip);
			}
		}

		return this;
	} ,

	/**
	 * Set multiple parameters
	 *
	 * @param {Object} params
	 * @returns {QueryBuilder}
	 */
	setParameters: function(params) {
		if (Array.isArray(params)) {
			this._params = params;
			return this;
		}

		for (var m in params) {
			this.setParameter(m, params[m]);
		}

		return this;
	} ,

	/**
	 * Set a single parameter
	 *
	 * @param {String} name The parameter name
	 * @param {String|Number} value The value
	 * @returns {QueryBuilder}
	 */
	setParameter: function(name, value) {
		this._params[name] = value;
		return this;
	} ,

	/**
	 * Get the parameters
	 *
	 * @returns {Array|Object}
	 */
	getParameters: function() {
		return this._params;
	} ,

	/**
	 * Get a single parameter
	 *
	 * @param {String} name
	 * @returns {String|Number|null}
	 */
	getParameter: function(name) {
		if (typeof this._params[name] !== 'undefined') {
			return this._params[name];
		}
		return null;
	} ,

	/**
	 * Set the max results for this query
	 *
	 * @param {Number} max
	 * @returns {QueryBuilder}
	 */
	setMaxResults: function(max) {
		max = parseInt(max, 10);
		if (!isNaN(max)) {
			this._maxResults = max;
			this._dirty();
		}
		return this;
	} ,

	/**
	 * Get the max results
	 *
	 * @returns {Number}
	 */
	getMaxResults: function() {
		return parseInt(this._maxResults, 10);
	} ,

	/**
	 * Set the OFFSET for the query (first position to start retrieving records)
	 *
	 * @param {Number} offset
	 * @returns {QueryBuilder}
	 */
	setOffset: function(offset) {
		this._offset = offset;
		this._dirty();
		return this;
	} ,

	/**
	 * Get the OFFSET for the query
	 *
	 * @returns {Number}
	 */
	getOffset: function() {
		return this._offset;
	} ,

	/**
	 * Set the distinct bool
	 *
	 * @param {Boolean|undefined} bool
	 * @returns {QueryBuilder}
	 */
	distinct: function(bool) {
		if (arguments.length === 0) {
			bool = true;
		}
		this._distinct = !!bool;
		this._dirty();
		return this;
	} ,

	/**
	 * See if the distinct flag is set
	 *
	 * @returns {Boolean}
	 */
	isDistinct: function() {
		return !!this._distinct;
	} ,

	/**
	 * Add an expression to the builder
	 *
	 * @param {String} name The part name
	 * @param {AbstractExpr|*} expr The expression / part
	 * @param {Boolean} append If true, data gets appended to this part, otherwise it is overwritten
	 * @returns {QueryBuilder}
	 * @throws Error
	 */
	'add': function(name, expr, append) {
		if (typeof this._parts[name] === 'undefined') {
			throw new Error('Invalid query part ' + name);
		}

		if (append && (name === 'where' || name === 'having')) {
			append = false;
		}

		if (append) {
			if (!Array.isArray(this._parts[name])) {

				if (this._parts[name]) {
					this._parts[name] = [this._parts[name], expr];
				} else {
					this._parts[name] = expr;
				}

			} else {
				this._parts[name].push(expr);
			}
		}
		else
		{
			this._parts[name] = expr;
		}

		this._dirty();

		return this;
	} ,

	/**
	 * Add a select fragment
	 *
	 * <code>
	 *     var qb = manager.createQueryBuilder()
	 *     			.select('name', 'status')
	 *     			.from('users');
	 * </code>
	 *
	 * @param {*} select The select expressions
	 * @returns {QueryBuilder}
	 */
	select: function(select) {

		if (arguments.length > 0 && !select) {
			return this;
		}

		this._queryType = QueryBuilder.TYPE_SELECT;

		return this.add('select', new Select(Array.isArray(select) ? select : Array.prototype.slice.call(arguments)));
	} ,

	/**
	 * Add a select fragment
	 *
	 * <code>
	 *     $qb = manager.createQueryBuilder()
	 *     			.select('o.*')
	 *     			.addSelect('u.*')
	 *     			.from('orders', 'o')
	 *     			.innerJoin('users', 'u', 'o.user_id = u.id')
	 * </code>
	 *
	 * @param {*} select The select expressions
	 * @returns {QueryBuilder}
	 */
	addSelect: function(select) {

		if (arguments.length > 0 && !select) {
			return this;
		}

		this._queryType = QueryBuilder.TYPE_SELECT;

		return this.add('select', new Select(Array.isArray(select) ? select : Array.prototype.slice.call(arguments)), true);
	} ,

	/**
	 * Add all the columns from a table to the select list
	 *
	 * <code>
	 *     manager.createQueryBuilder()
	 *     		.selectTable('users', 'u')
	 *     		.where('u.id = :id')
	 *     		.setParameter(':id', 5)
	 *     		.getQuery().getResult().then(function(results) { console.log(results); });
	 * </code>
	 *
	 * @param {String} table
	 * @param {String|undefined} alias
	 * @returns {QueryBuilder}
	 */
	selectTable: function(table, alias) {

		if (!table) {
			return this;
		}

		if (!alias) {
			alias = table;
		}

		var documentName = this._manager.mapper.mapCollectionNameToDocumentName(table);
		if (documentName) {
			// if we have a mapped document for this table (collection) then send it to 'selectDocument'
			return this.selectDocument(documentName, alias);
		}

		this.select(alias + '.*');

		// if this is the first select added, also add the from
		if (!this._parts.from) {
			this.from.apply(this, arguments);
		}

		return this;
	} ,

	/**
	 * Add all the columns from a Document (model / entity) to the select list
	 *
	 * <code>
	 *     manager.createQueryBuilder()
	 *     		.selectDocument('User', 'u')
	 *     		.where('u.id = :id')
	 *     		.setParameter(':id', 5)
	 *     		.getQuery().getResult().then(function(results) { console.log(results); });
	 * </code>
	 *
	 * @param {String} documentName The Document Name
	 * @param {String|undefined} alias The table alias to use in the select
	 * @param {String|undefined} collectionName The collection name (table) matching the Document
	 * @returns {QueryBuilder}
	 * @throws Error
	 */
	selectDocument: function(documentName, alias, collectionName) {

		if (!collectionName) {
			collectionName = this._manager.mapper.mapDocumentNameToCollectionName(documentName);
			if (!collectionName) {
				throw new Error('Unable to map ' + documentName + ' to a known collection');
			}
		}

		var fields = this._manager.getDocumentFields(documentName);
		if (!Array.isArray(fields) || fields.length === 0) {

			this.select(collectionName + '.*');

		} else {

			var names = [];
			var i, len = fields.length;
			for (i = 0; i < len; i++) {
				names.push(alias + '.' + fields[i].name);
			}

			this.addSelect(names);
		}

		if (!this._repositoryName) {
			this._repositoryName = documentName;
		}

		// if this is the first select added, also add the from
		if (!this._parts.from) {
			this.from(collectionName, alias);
		}

		return this;
	} ,

	/**
	 * Add an update fragment
	 *
	 * <code>
	 *     // simple update
	 *     var qb = manager.createQueryBuilder()
	 *     			.update('users')
	 *     			.set('password', md5('password'))
	 *     			.where('id = :user');
	 *
	 *     // update single record with multi table criteria
	 *     var qb = manager.createQueryBuilder()
	 *     			.update('users')
	 *     			.innerJoin('orders', 'o', 'users.id = o.user_id')
	 *     			.where('o.id = :id')
	 *     			.andWhere('u.id = :user');
	 *
	 *     // update multiple records
	 *     var qb = manager.createQueryBuilder()
	 *     			.update('users', 'orders')
	 *     			.innerJoin('orders', 'o', 'users.id = o.user_id')
	 *     			.where('o.id = :id')
	 *     			.andWhere('u.id = :user');
	 * </code>
	 *
	 * @param {String} table The table name
	 * @param {String} alias The table alias
	 * @returns {QueryBuilder}
	 */
	update: function(table, alias) {

		if (arguments.length > 0 && !table) {
			return this;
		}

		// register the repository name for the update
		if (!this._repositoryName) {
			var repositoryName = this._manager.mapper.mapCollectionNameToDocumentName(table);
			if (repositoryName) {
				this._repositoryName = repositoryName;
			}
		}

		this._queryType = QueryBuilder.TYPE_UPDATE;

		return this.from(table, alias);
	} ,

	/**
	 * Add a delete fragment
	 *
	 * <code>
	 *     // simple update
	 *     var qb = manager.createQueryBuilder()
	 *     			.delete('users')
	 *     			.where('id = :user');
	 * </code>
	 *
	 * @param {String} table The table name
	 * @param {String} alias The table alias
	 * @returns {QueryBuilder}
	 */
	'delete': function(table, alias) {
		if (arguments.length > 0 && !table) {
			return this;
		}

		// register the repository name for the update
		if (!this._repositoryName) {
			var repositoryName = this._manager.mapper.mapCollectionNameToDocumentName(table);
			if (repositoryName) {
				this._repositoryName = repositoryName;
			}
		}

		this._queryType = QueryBuilder.TYPE_DELETE;

		return this.from(table, alias);
	} ,

	/**
	 * Set the From fragment
	 *
	 * @param {String} table The table name
	 * @param {String} alias The table alias
	 * @returns {QueryBuilder}
	 */
	'from': function(table, alias) {
		return this.add('from', new From(table, alias));
	} ,

	/**
	 * Add a From fragment
	 *
	 * @param {String} table The table name
	 * @param {String} alias The table alias
	 * @returns {QueryBuilder}
	 */
	'addFrom': function(table, alias) {
		return this.add('from', new From(table, alias), true);
	} ,

	/**
	 * Add a SET expression
	 *
	 * @param {String|Object} name The column name to set OR an object of name => value to set
	 * @param {String|Number|*} value The value to set
	 * @returns {QueryBuilder}
	 */
	'set': function(name, value) {
		if (arguments.length === 1 && arguments[0] instanceof Object) {
			for (var m in arguments[0]) {
				var expr = arguments[0][m];
				if (expr.constructor.name === 'Comparison') {
					this.add('set', expr, true);
				} else {
					this.add('set', new Comparison(m, Comparison.EQ, ':' + m), true);
					this.setParameter(m, expr);
				}
			}
		} else {
			this.add('set', new Comparison(name, Comparison.EQ, value), true);
		}
		return this;
	} ,

	/**
	 * Add a WHERE expression
	 *
	 * @param {*} where
	 * @returns {QueryBuilder}
	 */
	where: function(where) {

		var andx = new Andx();

		if (arguments.length !== 1 && where.constructor.name !== 'Composite') {

			where = new Andx(Array.prototype.slice.call(arguments));

		} else if (arguments.length === 1 &&
					andx.allowedClasses.indexOf(where.constructor.name) === -1 &&
					where instanceof Object) {

			var empty = true;
			for (var m in arguments[0]) {
				empty = false;
				var expr = arguments[0][m];
				if (andx.allowedClasses.indexOf(expr.constructor.name) > -1) {
					andx.add(expr);
				} else {
					andx.add(new Comparison(m, Comparison.EQ, ':' + m));
					this.setParameter(m, expr);
				}
			}
			if (empty) {
				return this;
			}
			where = andx;
		}
		return this.add('where', where);
	} ,

	/**
	 * Add one or more WHERE expression, joining with AND
	 *
	 * @returns {QueryBuilder}
	 */
	andWhere: function() {
		var args = Array.prototype.slice.call(arguments);
		var where = this._parts.where;

		if (arguments.length === 1 &&
			arguments[0] instanceof Object) {

			var empty = true;
			var andx = new Andx();
			for (var m in arguments[0]) {
				empty = false;
				var expr = arguments[0][m];
				if (andx.allowedClasses.indexOf(expr.constructor.name) > -1) {
					andx.add(expr);
				} else {
					andx.add(new Comparison(m, Comparison.EQ, ':' + m));
					this.setParameter(m, expr);
				}
			}
			if (empty) {
				return this;
			}

			args = andx;
		}

		if (where.constructor.name === 'Andx') {

			where.addMultiple(args);

		} else {
			if (Array.isArray(args)) {
				args.unshift(where);
				where = new Andx(args);
			} else {
				where = new Andx([where, args]);
			}
		}

		return this.add('where', where);
	} ,

	/**
	 * Add one or more WHERE expression, joining with OR
	 *
	 * @returns {QueryBuilder}
	 */
	orWhere: function(args) {
		var isObject = false;
		var where = this._parts.where;

		if (arguments.length === 1 &&
			arguments[0] instanceof Object) {

			var empty = true;
			var orx = new Orx();
			for (var m in arguments[0]) {
				empty = false;
				var expr = arguments[0][m];
				if (orx.allowedClasses.indexOf(expr.constructor.name) > -1) {
					orx.add(expr);
				} else {
					orx.add(new Comparison(m, Comparison.EQ, ':' + m));
					this.setParameter(m, expr);
				}
			}
			if (empty) {
				return this;
			}

			isObject = true;
			args = orx;
		}

		if (where.constructor.name === 'Orx') {

			where.addMultiple(args);

		} else {
			if (!isObject) {
				args = Array.prototype.slice.call(arguments);
				args.unshift(where);
				where = new Orx(args);
			} else {
				where = new Orx([where, args]);
			}
		}

		return this.add('where', where);
	} ,

	/**
	 * Add a one or more group-by expression
	 *
	 * @returns {QueryBuilder}
	 */
	groupBy: function() {
		return this.add('groupBy', new GroupBy(Array.prototype.slice.call(arguments)));
	} ,

	/**
	 * Add one or more additional group-by expression (appended)
	 *
	 * @returns {QueryBuilder}
	 */
	addGroupBy: function() {
		return this.add('groupBy', new GroupBy(Array.prototype.slice.call(arguments)), true);
	} ,

	/**
	 * Add a HAVING expression to the query
	 *
	 * @param {*} having
	 * @returns {QueryBuilder}
	 */
	having: function(having) {
		if (arguments.length !== 1 &&
			having.constructor.name !== 'Andx' &&
			having.constructor.name !== 'Orx') {

			having = new Andx(Array.prototype.slice.call(arguments));
		}
		return this.add('having', having);
	} ,

	/**
	 * Add one or more additional having expression joining with AND
	 *
	 * @returns {QueryBuilder}
	 */
	andHaving: function() {
		var having = this._parts.having;

		if (having.constructor.name === 'Andx') {

			having.addMultiple(arguments);

		} else {
			var args = Array.prototype.slice.call(arguments);
			args.unshift(having);

			having = new Andx(args);
		}

		return this.add('having', having);
	} ,

	/**
	 * Add one or more additional having expression joining with OR
	 *
	 * @returns {QueryBuilder}
	 */
	orHaving: function() {
		var having = this._parts.having;

		if (having.constructor.name === 'Orx') {

			having.addMultiple(arguments);

		} else {
			var args = Array.prototype.slice.call(arguments);
			args.unshift(having);

			having = new Orx(args);
		}

		return this.add('having', having);
	} ,

	/**
	 * Add an order-by expression
	 *
	 * @param {*} sort
	 * @param {*} order
	 * @param {Boolean} append Behaves like addOrderBy, appends to the existing order-by
	 * @returns {QueryBuilder}
	 */
	orderBy: function(sort, order, append) {
		var expr;

		if (sort.constructor.name === 'OrderBy') {
			expr = sort;
		} else {
			expr = new OrderBy(sort, order);
		}

		return this.add('orderBy', expr, !!append);
	} ,

	/**
	 * Append an order-by expression
	 *
	 * @param {*} sort
	 * @param {*} order
	 * @returns {QueryBuilder}
	 */
	addOrderBy: function(sort, order) {
		return this.orderBy(sort, order, true);
	} ,

	/**
	 * Inner join a table / document
	 *
	 * @param {String} joinType The join type (Join.TYPE_INNER, Join.TYPE_LEFT, etc.)
	 * @param {String} table The table name to join
	 * @param {String|null|undefined} alias The joining table's alias
	 * @param {AbstractExpr|Comparison|String|null|undefined|*} condition The condition to join on
	 * @returns {QueryBuilder}
	 * @throws Error
	 */
	join: function(joinType, table, alias, condition) {

		// TODO : finish

		var self = this,
			tableMap = this.getTableMap(),
			parentAlias;

		if (condition && condition.constructor.name === 'Comparison') {

			parentAlias = this.parseTableAlias(condition.getLeftExpr());
			if (!parentAlias) {

				parentAlias = this.parseTableAlias(condition.getRightExpr());
			}

		} else if (condition instanceof String || typeof condition === 'string') {

			// check all tables in the condition (tbl.field = tbl.field or tbl.field = tbl.field)
			var conParts = condition.match(/^([a-z_]+\.[a-z_]+)|\s*([a-z_]+\.[a-z_]+)/ig);
			conParts.forEach(function(part) {
				var check = self.parseTableAlias(part);
				if (check) {
					parentAlias = check;
					return false;
				}
				return true;
			});

		} else {

			/* TODO : left off here - got sql relational mapping working for now

			var findRelation = function(docName) {
				for (var type in metadata.relations) {
					for (var n in metadata.relations[type]) {
						if (metadata.relations[type][n].document === docName) {

						}
					}
				}
			};

			var docName, metadata;
			var relationFields;
			for (var t in tableMap) {
				docName = this._manager.mapper.mapCollectionNameToDocumentName(tableMap[t].table);
				if (docName) {
					metadata = this._manager.registry.getMetadataByName(docName);
				}
			}

			metadata = this._manager.registry.getMetadataByName(this._repositoryName || self.getRootAlias() || table);

			if (metadata) {

				console.log(metadata.relations['one-to-one']);

				for (var type in metadata.relations) {
					for (var n in metadata.relations[type]) {

						//if (metadata.relations[type][n].document === )
					}
				}

				relationFields = metadata.getRelationFields() || [];

				if (relationFields && relationFields.length > 0) {
					console.log(relationFields);

					for (var type in relationFields.relations) {
						console.log(type);
						for (var n in relationFields.relations[type]) {
							console.log(n);
							console.log(relationFields.relations[type][n]);
						}
					}
				}
			}*/
		}

		if (!parentAlias) {
			// TODO : we need to find the root alias based on mapped metadata

			throw new Error('Unable to find joining table for "' + table + '"');
		}

		var joiningAlias = this.findJoiningAlias(alias, parentAlias);
		if (!joiningAlias) {
			// we couldn't find the table name / alias to join against
			throw new Error('Unable to find joining table for "' + table + '"');
		}

		if (!condition) {
			// we need to generate our condition based on mapped metadata for this table
			if (typeof tableMap[joiningAlias] !== 'undefined') {
				//condition = new Comparison();
				// TODO : left off here, need to generate a condition
			}
			throw new Error('You must provide a condition to join on');
		}

		// we might have the document name, make sure we know what collection it goes to
		var collectionName = this._manager.mapper.mapDocumentNameToCollectionName(table) || table;

		var join = new Join(joinType, collectionName, alias, condition);
		var sql = join.toString();

		var map = {
			toString: function() {
				return sql;
			}
		};
		map[joiningAlias] = join;

		this.add('join', map, true);

		return this;
	} ,

	/**
	 * Add an inner join
	 *
	 * @param {String} table The table name to join
	 * @param {String|null|undefined} alias The joining table's alias
	 * @param {AbstractExpr|Comparison|String|null|undefined|*} condition The condition to join on
	 * @returns {QueryBuilder}
	 */
	innerJoin: function(table, alias, condition) {
		return this.join(Join.TYPE_INNER, table, alias, condition);
	} ,

	/**
	 * Add a left join
	 *
	 * @param {String} table The table name to join
	 * @param {String|null|undefined} alias The joining table's alias
	 * @param {AbstractExpr|Comparison|String|null|undefined|*} condition The condition to join on
	 * @returns {QueryBuilder}
	 */
	leftJoin: function(table, alias, condition) {
		return this.join(Join.TYPE_LEFT, table, alias, condition);
	} ,

	/**
	 * Get the string representation of this QueryBuilder (the SQL)
	 *
	 * @returns {String}
	 */
	toString: function() {
		return this.getSql();
	}
};



QueryBuilder.prototype.constructor = QueryBuilder;

module.exports = QueryBuilder;