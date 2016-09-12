(function() {
    'use strict';


    const Class         = require('ee-class')
    const Waiter        = require('ee-waiter')
    const log           = require('ee-log')
    const type          = require('ee-types')
    const Arguments     = require('ee-arguments')
    const ResourcePool  = require('ee-resource-pool')
    const request       = require('request')
    const xml2json      = require('ee-xml-to-json')
    const rules         = require('../transformations/list');





    module.exports = new Class({

          pool: {}
        , max: { default: 10 }


        , init: function(options) {
            options = options || {};

            if (!type.string(options.key))      throw new Error('missing the string property «key» on the options object!').setName('MissingArgumentException');
            if (!type.string(options.secret))   throw new Error('missing the string property «secret» on the options object!').setName('MissingArgumentException');
            if (!type.string(options.bucket))   throw new Error('missing the string property «bucket» on the options object!').setName('MissingArgumentException');


            this.credentials = {
                  key:      options.key
                , secret:   options.secret
                , bucket:   options.bucket
            };

            // limits
            if (type.number(options.maxConcurrent))             this.max.generic    = options.maxConcurrent;
            if (type.number(options.maxConcurrentDownloads))    this.max.download   = options.maxConcurrentDownloads;
            if (type.number(options.maxConcurrentUploads))      this.max.upload     = options.maxConcurrentUploads;
            if (type.number(options.maxConcurrentLists))        this.max.list       = options.maxConcurrentLists;
            if (type.number(options.maxConcurrentDeletes))      this.max.delete     = options.maxConcurrentDeletes;

            // initialize pools
            this.initialize();
        }





        , list: function() {
            this.execute('list', arguments);
        }





        , listCommonPrefixes: function(prefix, delimiter, callback, offset) {
            // Added by Garry Lachman (garry@lachman.co) https://github.com/garrylachman
            this.execute('list', [prefix, callback, offset, delimiter]);
        }





        , _list: function(path, callback, offset, delimiter) {
            const isCommonPrefixesCommand = (delimiter != undefined);

            request({
                  url:          'https://' + this.credentials.bucket + '.s3.amazonaws.com/?prefix=' + path.substr(1) + (isCommonPrefixesCommand ? ('&delimiter='+delimiter) : '')
                , method:       'GET'
                , aws:          this.credentials
                , encoding:     null
                , timeout:      60000 // 60 secs
            }, (err, response, body) => {
                if (err) callback (err);
                else {
                    if (response && response.statusCode === 200) {
                        xml2json(body, rules, (err, data) => {
                            if (err) callback(err);
                            else {
                                let next;

                                if (data && data.contents && data.contents.length > 0) {
                                    data.contents.forEach(function(obj) {
                                        obj.file = obj.key.substr(obj.key.lastIndexOf('/') +1);
                                    });
                                }

                                if (data && data.commonPrefixes && data.commonPrefixes.length > 0) {
                                    data.commonPrefixes = data.commonPrefixes.map(function(obj) {
                                        return obj.prefix;
                                    });
                                }

                                if (data && data.truncated) {
                                    next = (newCallback) => {
                                        this.list(path, newCallback, data.contents[data.contents.length - 1].key, delimiter);
                                    };
                                }

                                data = isCommonPrefixesCommand ? data.commonPrefixes : data.contents;

                                callback(null, data, next);
                            }
                        });
                    }
                    else callback(new Error('Listing failed, status: '+response.statusCode).setName('ListingFailedException'));
                }
            });
        }




        , delete: function(path, callback) {

            // deleting a file or a directory?
            if (path && path.length > 0 && path.substr(path.length - 1, 1) === '/') {

                const deleteList = (err, list, next) => {
                    if (list && list.length > 0) {
                        const deleteQueue = new Waiter();

                        list.forEach((item) => {
                            deleteQueue.add((cb) => {
                                this.delete('/' + item.key, cb );
                            });
                        });

                        deleteQueue.start((err) => {
                            if (err) callback(err);
                            else if (next) next(deleteList);
                            else callback();
                        });
                    }
                    else callback();
                };

                // delete a entire directories
                this.list(path, deleteList);
            }
            else this.execute('delete', arguments);
        }





        , _delete: function(path, callback, url) {
            request({
                  url:          'https://' + this.credentials.bucket + '.s3.amazonaws.com' + path
                , method:       'DELETE'
                , aws:          this.credentials
                , encoding:     null
                , timeout:      60000 // 60 secs
            }, (err, response, body) => {
                if (err) callback (err);
                else {
                    if (response) {
                         if (response.statusCode === 204) callback(null, response.headers);
                         else if (response.statusCode === 307) this._delete(path, callback, response.headers.location);
                         else callback(new Error('Deletion failed, status: '+response.statusCode).setName('DeletionFailedException'));
                    }
                    else callback(new Error('Deletion failed, unknown status!').setName('DeletionFailedException'));
                }
            });
        }




        , head: function() {
            this.execute('head', arguments);
        }

        , _head: function(path, callback, url) {
            request({
                  url:          url || ('https://' + this.credentials.bucket + '.s3.amazonaws.com' + path)
                , method:       'HEAD'
                , aws:          this.credentials
                , encoding:     null
                , timeout:      60000 // 60 secs
            }, (err, response, body) => {
                if (err) callback (err);
                else {
                    if (response) {
                        if (response.statusCode === 200) callback(null, response.headers);
                        else if (response.statusCode === 307) this._head(path, callback, response.headers.location);
                        else callback(new Error('Download failed, status: '+response.statusCode).setName('DownloadFailedException'), response.statusCode);
                    }
                    else callback(new Error('Download failed, unknown status!').setName('DownloadFailedException'), response.statusCode);
                }
            });
        }





        , get: function() {
            this.execute('get', arguments);
        }

        , _get: function(path, callback, url) {
            request({
                  url:          url || ('https://' + this.credentials.bucket + '.s3.amazonaws.com' + path)
                , method:       'GET'
                , aws:          this.credentials
                , encoding:     null
                , timeout:      60000 // 60 secs
            }, (err, response, body) => {
                if (err) callback (err);
                else {
                    if (response) {
                        if (response.statusCode === 200) callback(null, body, response.headers);
                        else if (response.statusCode === 307) this._get(path, callback, response.headers.location);
                        else callback(new Error('Download failed, status: '+response.statusCode).setName('DownloadFailedException'), response.statusCode);
                    }
                    else callback(new Error('Download failed, unknown status!').setName('DownloadFailedException'), response.statusCode);
                }
            });
        }





        , put: function() {
            this.execute('put', arguments);
        }

        , _put: function() {
            const arg = new Arguments(arguments);
            const path          = arg.get('string');
            const data          = arg.get('buffer');
            const contentType   = arg.getByIndex('string', 1);
            const isPrivate     = arg.get('boolean', true);
            const headers       = arg.get('object', {});
            const callback      = arg.get('function', function(err) { if (err) throw new err; });

            if (!path) callback(new Error('missing the argument «path», it must be the first string variable passed to the put method!').setName('MissingArgumentException'));
            if (!data) callback(new Error('missing the argument «data», you must pass a variable with the type «buffer» to the put method!').setName('MissingArgumentException'));
            if (!contentType) callback(new Error('missing the argument «contentType», it must be the second string variable passed to the put method!').setName('MissingArgumentException'));

            // set setome headers
            headers['Content-Type'] = contentType;
            if(!headers.date) headers.date = new Date().toUTCString();
            if (!isPrivate) headers['x-amz-acl'] = 'public-read';


            const executeRequest = (url) => {
                request({
                      url:          url || ('https://' + this.credentials.bucket + '.s3.amazonaws.com' + path)
                    , method:       'PUT'
                    , aws:          this.credentials
                    , body:         data
                    , timeout:      600000 // 10 minutes
                    , headers:      headers
                }, (err, response, body) => {
                    if (err) callback(err);
                    else {
                        if (response) {
                            if (response.statusCode === 200) callback();
                            else if (response.statusCode === 307) executeRequest(response.headers.location);
                            else callback(new Error('Upload failed, status: ' + response.statusCode).setName('UploadFailedException'));
                        }
                        else callback(new Error('Upload failed, unknown status!').setName('UploadFailedException'));
                    }
                });
            };

            executeRequest();
        }






        , execute: function(action, origArgs) {
            const callback  = new Arguments(origArgs).get('function', function() {});
            let args = Array.prototype.slice.call(origArgs, 0);

            this.pool[action].get((err, resource) => {
                if (err) callback(err);
                else {
                    // we extracted the original callback so we can add our own
                    // thats we neded to free the resource afer the upload has finished
                    // the freeResource function was added by the resourcepool and isnt
                    // part of the uploader class
                    let idx = 0;
                    const cb = function() {
                        const returnValues = Array.prototype.slice.call(arguments, 0);

                        resource.freeResource();

                        callback.apply(null, returnValues);
                    }.bind(this);


                    // remove original callback from
                    args = args.filter(function(a, index) {
                        if (type.function(a)) {
                            idx = index;
                            return false;
                        }
                        else {
                            return true;
                        }
                    });

                    args.splice(idx, 0, cb);

                    this['_' + action].apply(this, args);
                }
            });
        }



        , initialize: function() {
            ['get', 'put', 'delete', 'list', 'head', 'listCommonPrefixes'].forEach((action) => {
                this.createResourPool(action);
            });
        }



        , createResourPool: function(action) {
            const pool = new ResourcePool({
                  max:                  this.max[action] || this.max.generic || this.max.default
                , maxWaitingRequests:   100000
                , timeout:              3600000
                , idle:                 60000
                , prefetch:             10
            });

            pool.on('resourceRequest', function(callback) { callback({}); });

            this.pool[action] = pool;
        }
    });
})();
