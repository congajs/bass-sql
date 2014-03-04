/*
 * This file is part of the bass library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// local modules
var QueryClient = require('./query/query-client');

// expressions
var Select = require('./query/expr/select');
var From = require('./query/expr/from');
var Comparison = require('./query/expr/comparison');
var Andx = require('./query/expr/andx');
var Orx = require('./query/expr/orx');
var GroupBy = require('./query/expr/group-by');
var OrderBy = require('./query/expr/order-by');

/**
 * The Query Builder
 *
 * @param {Manager} manager
 * @constructor
 */
function QueryBuilder(manager) {
	if (manager) {
		this._em = manager;
		this._params = {};
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
	 */
	_state: QueryBuilder.STATE_CLEAN ,

	/**
	 * The Query Type
	 *
	 * @type {String}
	 */
	_queryType: null ,

	/**
	 * The Bass Manager
	 *
	 * @type {Manager}
	 */
	_em: null ,

	/**
	 * The offset for the query
	 *
	 * @type {Number}
	 */
	_offset: null ,

	/**
	 * Parameters for the query
	 *
	 * @type {Array|Object}
	 */
	_params: null ,

	/**
	 * Max results for the query (the "limit")
	 *
	 * @type {Number}
	 */
	_maxResults: null ,

	/**
	 * Flag for a DISTINCT query
	 *
	 * @type {Boolean}
	 */
	_distinct: false ,

	/**
	 * Query parts / expressions
	 *
	 * @type {Object}
	 */
	_parts: null ,

	/**
	 * Cached SQL from the last time it was generated
	 *
	 * @type {String}
	 */
	_sql: null ,

	/**
	 * The known repository / document name
	 *
	 * @type {String|null}
	 */
	_repositoryName: null ,

	// TODO : Expr helper class and expression builder
	expr: function() {

		//return this._em.getExpressionBuilder();
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
		return this._em;
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

		this._state = QueryBuilder.STATE_CLEAN;
		this._sql = sql;

		return sql;
	} ,

	/**
	 * Get the SQL for a SELECT query
	 *
	 * @returns {String}
	 * @private
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
	 * @private
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
	 * @private
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
	 * @private
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
		return (new QueryClient(this._em, query))
			.setRepositoryName(this._repositoryName || null)
			.setSql(this.getSql() || null)
			.setOffset(this.getOffset() || null)
			.setMaxResults(this.getMaxResults() || null)
			.setParameters(this.getParameters());
	} ,

	/**
	 * Import data from a Query object
	 *
	 * @param {Query} query
	 * @returns {QueryBuilder}
	 */
	importQuery: function(query) {
		if (!query) {
			return this;
		}

		if (typeof query.getSort === 'function') {
			var sort = query.getSort();
			for (var sortKey in sort) {
				var sortDir = sort[sortKey] === -1 || sort[sortKey] === '-1' ? 'DESC' : 'ASC';

				this.addOrderBy(sortKey, sortDir)
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

		if (typeof query.getConditions === 'function') {
			this.andWhere(query.getConditions());
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
			this._state = QueryBuilder.STATE_DIRTY;
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
		this._state = QueryBuilder.STATE_DIRTY;
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
		this._state = QueryBuilder.STATE_DIRTY;
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

		this._state = QueryBuilder.STATE_DIRTY;

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

		var repositoryName = this._em.mapper.mapCollectionNameToDocumentName(table);
		if (repositoryName) {
			var fields = this._em.getDocumentFields(repositoryName);
			if (!fields || (Array.isArray(fields) && fields.length === 0)) {
				return this.select(table + '.*');
			}

			var names = [];
			var i, len = fields.length;
			for (i = 0; i < len; i++) {
				names.push(alias + '.' + fields[i].name);
			}

			this.addSelect(names);

			if (!this._repositoryName) {
				this._repositoryName = repositoryName;
			}

		} else {

			return this.select(table + '.*');
		}

		this.from.apply(this, arguments);

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
			var repositoryName = this._em.mapper.mapCollectionNameToDocumentName(table);
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
			var repositoryName = this._em.mapper.mapCollectionNameToDocumentName(table);
			if (repositoryName) {
				this._repositoryName = repositoryName;
			}
		}

		this._queryType = QueryBuilder.TYPE_DELETE;

		return this.from(table, alias);
	} ,

	/**
	 * Add a From fragment
	 *
	 * @param {String} table The table name
	 * @param {String} alias The table alias
	 * @returns {QueryBuilder}
	 */
	'from': function(table, alias) {
		return this.add('from', new From(table, alias));
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