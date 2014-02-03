/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Client = function(db){
	this.db = db;
};

Client.prototype = {

	/**
	 * Insert a new document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert: function(metadata, collection, data, cb){

		var sql = 'INSERT INTO ' + collection + ' SET ?';

		this.db.connection.query(sql, data, function(err, result) {

			if (err){
				cb(err, null);
				return;
			}

			data[metadata.getIdFieldName()] = result.lastInsertId;
			cb(err, data);
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
	update: function(metadata, collection, id, data, cb){

		// need to remove the id from the update data
		delete data[metadata.getIdFieldName()];

		var sql = "update " + collection + " set ";
		var numProps = Object.keys(data).length;
		var x = 0;
		var params = [];

		for (var i in data){
			params.push(data[i]);
			sql += " " + i + " = ?";
			if (x < numProps-1){
				sql += ", ";
			}
			x++;
		}

		sql += " where " + metadata.getIdFieldName() + " = ?";
		params.push(id);

		this.db.connection.query(sql, params, function(err, result){
			cb(err, result);
		});
	},

	/**
	 * Remove a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove: function(metadata, collection, id, cb){

		var sql = "delete from " + collection + " where " + metadata.getIdFieldName() + " = ?";
		var params = [id];

		this.db.connection.query(sql, params, function(err, result){
			cb(err, result);
		});
	},

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find: function(metadata, collection, id, cb){

		var fields = [];

		metadata.fields.forEach(function(el){
			fields.push(el.name);
		});

		var sql = "select " + fields.join(",") + 
					" from " + collection + " where " + 
					metadata.getIdFieldName() + " = ?";

		this.db.connection.query(sql, [id], function(err, result){

			if (err){

				cb(err, null);

			} else {

				var rows = result.rows;

				if (rows.length === 0){
					result = null;
				} else {
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
	findByQuery: function(metadata, collection, query, cb){

		this.findBy(metadata, collection, query.getConditions(), query._sort, query.getSkip(), query.getLimit(), cb);
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
	findCountByQuery: function(metadata, collection, query, cb){

		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll){

			cursor = coll.find(mongoCriteria).count(function(err, count){
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
	findBy: function(metadata, collection, criteria, sort, skip, limit, cb){

		console.log(criteria);

		var fields = [];

		metadata.fields.forEach(function(el){
			fields.push(el.name);
		});

		var sql = "select " + fields.join(", ") + 
					" from " + collection;

		var params = [];

		if (Object.keys(criteria).length > 0){

			sql += " where ";

			var parts = [];

			for (var i in criteria){
				parts.push(i + " = ?");
				params.push(criteria[i]);
			}

			sql += parts.join(' and ');
		}

		if (typeof sort !== 'undefined' && sort !== null){

			var sorts = [];

			for (var i in sort){
				if (sort[i] === -1){
					sorts.push(i + ' desc ');
				} else {
					sorts.push(i + ' asc ');
				}
			}

			if (sorts.length > 0){
				sql += " order by " + sorts.join(', ');				
			}
		}

		if (skip || limit){

			if (limit){
				sql += ' LIMIT ' + parseInt(limit);
			}

			if (skip && limit){
				sql += ' OFFSET ' + parseInt(skip);
			}
		}

		this.db.connection.query(sql, params, function(err, result){

			if (err){
				cb(err);
			} else {
				var rows = result.rows;

				if (rows.length === 0){
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
	 * @return {[type]}              [description]
	 */
	create: function(metadata, collection, cb){

		this.db.createCollection(collection, cb);
	},

	/**
	 * Drop a collection
	 * 
	 * @param  {String}   collection
	 * @param  {Function} cb
	 * @return {void}
	 */
	drop: function(metadata, collection, cb){

		this.db.collection(collection, function(err, coll){
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
	rename: function(metadata, collection, newName, cb){
		this.db.collection(collection, function(err, coll){
			coll.rename(newName, cb);
		});
	},

	/**
	 * Get a list of all of the collection names in the current database
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections: function(cb){
		this.db.collections(cb);
	},

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 * 
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria: function(query){

		var newQuery = {};

		var conditions = query.getConditions();

		for (var field in conditions){

			if (typeof conditions[field] === 'object'){

				var tmp = {};

				for (var i in conditions[field]){
					tmp['$' + i] = conditions[field][i];
				}

				newQuery[field] = tmp;

			} else {
				newQuery[field] = conditions[field];
			}
		}

		return newQuery;
	}
};

module.exports = Client;