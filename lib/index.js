// index.js
//
// Databank driver for LevelDB
//
// Copyright 2012, StatusNet Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var databank = require('databank'),
    leveldb = require('leveldb'),
    Databank = databank.Databank,
    NotConnectedError = databank.NotConnectedError;

var LevelDBDatabank = function(params) {
    this.handle = null;
    this.file   = params.file || "/tmp/databank-leveldb";
    this.schema = params.schema || {};
};

LevelDBDatabank.prototype = new Databank();
LevelDBDatabank.prototype.constructor = LevelDBDatabank;

LevelDBDatabank.toKey = function(type, id) {
    return type + ":" + id;
};

LevelDBDatabank.prototype.connect = function(params, callback) {
    var bank = this;

    leveldb.open(bank.file, { create_if_missing: true }, function(err, handle) {
        if (err) {
            callback(err);
        } else if (!handle) {
            callback(new Error("No HANDLE returned"));
        } else {
            bank.handle = handle;
            callback(null);
        }
    });
};

LevelDBDatabank.prototype.disconnect = function(callback) {
    callback(null);
};

LevelDBDatabank.prototype.create = function(type, id, value, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }
    
    bank.handle.put(key, JSON.stringify(value), function(err) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, value);
        }
    });
};

LevelDBDatabank.prototype.read = function(type, id, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }

    bank.handle.get(key, function(err, value) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, JSON.parse(value));
        }
    });
};

LevelDBDatabank.prototype.update = function(type, id, value, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }

    bank.handle.put(key, JSON.stringify(value), function(err) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, value);
        }
    });
};

LevelDBDatabank.prototype.del = function(type, id, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError());
        return;
    }

    bank.handle.del(key, {}, callback);
};

module.exports = LevelDBDatabank;
