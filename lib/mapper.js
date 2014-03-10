/*
 * This file is part of the bass-sql library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
var async = require('async');


var Mapper = function(registry, client){
	this.registry = registry;
	this.client = client;
};

Mapper.prototype = {

	/**
	 * Convert a Javascript value to a db value
	 *
	 * @param {String} type
	 * @param  {*} value
	 * @return {*}
	 */
	convertJavascriptToDb: function(type, value){

		var converted = value;

		if (type.toLowerCase() === 'object'){
			converted = JSON.stringify(value);
		}

		return converted;
	},

	/**
	 * Convert a db value to a Javascript value
	 *
	 * @param {String} type
	 * @param {*} value
	 * @return {*}
	 */
	convertDbToJavascript: function(type, value){

		var converted = value;

		if (type.toLowerCase() === 'object'){
			try {
				converted = JSON.parse(value);
			} catch (e) {
				converted = null;
			}

		}

		if (type.toLowerCase() === 'boolean'){
			converted = Boolean(value);
		}

		return converted;
	},

	convertRelationsToData: function(metadata, model, data, cb) {

		var i,
			relation,
			relationMetadata,
			scalarReg = /boolean|string|number/;

		// one-to-one
		for (i in metadata.relations['one-to-one']) {

			relation = metadata.relations['one-to-one'][i];
			relationMetadata = this.registry.getMetadataByName(relation.document);

			// TODO: this assumes that the relationship is by the id-field - we need a way to specify the field it points to
			if (typeof model[i] !== 'undefined' && model[i] !== null) {
				if (model[i] instanceof Object) {
					if (typeof model[i].id !== 'undefined') {
						data[relation.field] = model[i].id;
					}
				} else if (scalarReg.test(typeof model[i])) {
					data[relation.field] = model[i];
				}
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){
			relation = metadata.relations['one-to-many'][i];
			relationMetadata = this.registry.getMetadataByName(relation.document);

			data[relation.field] = [];

			if (typeof model[i] !== 'undefined' && model[i]) {
				model[i].forEach(function(oneToManyDoc){

					// TODO: this assumes that the relationship is by the id-field - we need a way to specify the field it points to
					if (oneToManyDoc instanceof Object) {
						if (typeof oneToManyDoc.id !== 'undefined') {
							data[relation.field].push( oneToManyDoc.id );
						}
					} else if (scalarReg.test(typeof oneToManyDoc)) {
						data[relation.field].push( oneToManyDoc );
					}
				});
			}
		}

		if (typeof cb === 'function') {
			cb();
		}
	},

	convertDataRelationToDocument: function(metadata, fieldName, data, model, mapper, cb) {

		var field = metadata.getFieldByProperty(fieldName);

		if (!field || typeof data[field.name] === 'undefined' || data[field.name] === null) {
			cb(null, model);
			return;
		}

		var relation = metadata.getRelationByFieldName(fieldName);
		var relationMetadata = this.registry.getMetadataByName(relation.document);

		if (typeof metadata.relations['one-to-one'][fieldName] !== 'undefined') {

			// one-to-one, so fetch the related entity by id
			// TODO: this assumes that the relationship is by the id-field - we need a way to specify the field it points to
			this.client.find(relationMetadata, relationMetadata.collection, data[field.name], function(err, result) {
				model[relation.field] = result;
				cb(err, model);
			});

		} else if (typeof metadata.relations['one-to-many'][fieldName] !== 'undefined') {

			var ids = [];
			var relationVal;

			for(var j in data[fieldName]) {

				relationVal = data[fieldName][j];

				if (typeof relationVal instanceof Object) {
					if (typeof relationVal.id !== 'undefined') {

						ids.push(relationVal.id);

					} else if (typeof relationVal.getId === 'function') {

						ids.push(relationVal.getId());

					}
				} else if ((/boolean|string|number/).test(typeof relationVal)) {

					ids.push(relationVal);

				}
			}

			if (ids.length > 0) {

				// TODO: this assumes that the relationship is by the id-field - we need a way to specify the field it points to
				self.client.findWhereIn(relationMetadata.collection, 'id', ids, {id:-1}, function(err, results) {
					model[fieldName] = results;
					cb(err, model);
				});

			} else {
				cb(null, model);
			}
		}

	} ,

	convertDataRelationsToDocument: function(metadata, data, model, mapper, cb){

		var i;
		var self = this;
		var calls = [];

		// one-to-one
		for (i in metadata.relations['one-to-one']){

			if(typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, idx){

					calls.push(

						function(callback){

							self.convertDataRelationToDocument(metadata, i, data, model, mapper, function(err, item) {
								callback(model);
							});
						}
					);

				}(data, model, i));				
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){

			if(typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, i){

					calls.push(

						function(callback) {

							self.convertDataRelationToDocument(metadata, i, data, model, mapper, function(err, item) {
								callback(model);
							});
						}
					);

				}(data, model, i));
			}
		}

		if (calls.length > 0){

			// run all queries and process data
			async.parallel(calls, function(document){
				cb(null, document);
			});

		} else {
			cb(null, model);
		}
	}
};

module.exports = Mapper;