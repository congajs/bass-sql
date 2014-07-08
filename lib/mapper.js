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


function Mapper(registry, client) {
	this.registry = registry;
	this.client = client;
}

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

		switch (type.toLowerCase()) {
			case 'array' :
			case 'object' :
				if (value !== undefined && value !== null && !(/^(boolean|string|number)$/.test(typeof value))) {
					try {
						converted = JSON.stringify(value);
					} catch (e) {
						console.log(e.stack);
						converted = null;
					}
				}
				break;

			case 'json' :
				try {
					converted = JSON.stringify(value);
				} catch(e) {
					console.log(e.stack);
					converted = null;
				}
				break;

			case 'boolean' :
				// boolean value for sql is 1 or 0
				converted = !!value ? 1 : 0;
				break;
		}

		return converted;
	},

	/**
	 * Convert a db value to a Javascript value
	 *
	 * @param {String} type The field type
	 * @param {*} value The value
	 * @return {*} The converted value
	 */
	convertDbToJavascript: function(type, value){

		var converted = value;

		switch (type.toLowerCase()) {
			case 'array' :
			case 'object' :
			case 'json' :
				try {
					converted = JSON.parse(value);
				} catch (e) {
					if (value !== null && value.toString().length > 0) {
						console.log(e.stack);
						//console.log(value);
					}
					converted = null;
				}
				break;

			case 'boolean' :
				converted = Boolean(value);
				break;
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
			cb(null, data);
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
				if (!err) {
					model[relation.field] = result;
				}
				cb(err, model);
			});

		} else if (typeof metadata.relations['one-to-many'][fieldName] !== 'undefined') {

			var ids = [];
			var relationVal;
			var annotation = metadata.relations['one-to-many'][fieldName];

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

				var sort;
				if (annotation.sort && annotation.direction) {

					sort = {};
					sort[annotation.sort] = annotation.direction.toString().toLowerCase() === 'desc' ? -1 : 1;

				} else {
					sort = {id:-1};
				}

				// TODO: this assumes that the relationship is by the id-field - we need a way to specify the field it points to
				self.client.findWhereIn(relationMetadata, 'id', ids, sort, null, function(err, results) {
					if (!err) {
						model[fieldName] = results;
					}
					cb(err, model);
				});

			} else {
				cb(null, model);
			}

		} else {

			cb(null, model);
		}
	} ,

	convertDataRelationsToDocument: function(metadata, data, model, mapper, cb){

		var i;
		var self = this;
		var calls = [];

		// one-to-one
		for (i in metadata.relations['one-to-one']){

			if (typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, idx){
					calls.push(
						function(callback){
							self.convertDataRelationToDocument(metadata, idx, data, model, mapper, function(err, item) {
								callback(err, item);
							});
						}
					);
				}(data, model, i));
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){

			if (typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, idx){
					calls.push(
						function(callback) {
							self.convertDataRelationToDocument(metadata, idx, data, model, mapper, function(err, item) {
								callback(err, item);
							});
						}
					);
				}(data, model, i));
			}
		}

		if (calls.length > 0){

			// run all queries and process data
			async.parallel(calls, function(err, document){
				cb(err, document);
			});

		} else {
			cb(null, model);
		}
	}
};

Mapper.prototype.constructor = Mapper;

module.exports = Mapper;