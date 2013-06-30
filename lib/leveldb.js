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

var _ = require("underscore-contrib"),
    databank = require('databank'),
    levelup = require('levelup'),
    leveldown = require('leveldown'),
    crypto = require('crypto'),
    fs = require('fs'),
    path = require('path'),
    os = require('os'),
    Databank = databank.Databank,
    NotConnectedError = databank.NotConnectedError,
    AlreadyConnectedError = databank.AlreadyConnectedError,
    AlreadyExistsError = databank.AlreadyExistsError,
    NoSuchThingError = databank.NoSuchThingError;

var LevelDBDatabank = function(params) {
    this.handle = null;
    this.file   = params.file || "/tmp/databank-leveldb";
    this.schema = params.schema || {};
    this.mktmp  = params.mktmp || false;
};

LevelDBDatabank.prototype = new Databank();
LevelDBDatabank.prototype.constructor = LevelDBDatabank;

LevelDBDatabank.toKey = function(type, id) {
    return type + ":" + id;
};

LevelDBDatabank.prototype.connect = function(params, callback) {
    var bank = this,
        realConnect = function() {
            levelup(bank.file, { valueEncoding: 'json' }, function(err, handle) {
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

    if (bank.handle) {
        callback(new AlreadyConnectedError());
        return;
    }

    if (bank.mktmp) {
        bank.makeTempDir(function(err) {
            if (err) {
                callback(err);
            } else {
                realConnect();
            }
        });
    } else {
        realConnect();
    }
};

var tmpDir = function() {
    var tmpdir = (_.isFunction(os.tmpdir)) ? os.tmpdir() :
            (_.isFunction(os.tmpDir)) ? os.tmpDir() : null;

    if (tmpdir) {
        return tmpdir;
    } else {
        // XXX: check for C:\\Temp, C:\\Windows\Temp, all that jazz
        return "/tmp";
    }
};

LevelDBDatabank.prototype.makeTempDir = function(callback) {
    var bank = this,
        tmp = tmpDir(),
        tryAgain = function() {
            randomString(16, function(err, rs) {
                var dirname;
                if (err) {
                    callback(err);
                } else {
                    dirname = path.join(tmp, rs);
                    fs.stat(dirname, function(err, stats) {
                        if (err && err.code == 'ENOENT') {
                            bank.file = dirname;
                            callback(null);
                        } else if (err) {
                            callback(err);
                        } else {
                            // XXX: bounded retries; 10?
                            tryAgain();
                        }
                    });
                }
            });
        },
        randomString = function(bytes, cb) {
            crypto.randomBytes(bytes, function(err, buf) {
                var str;
                if (err) {
                    cb(err, null);
                } else {
                    str = buf.toString("base64");

                    str = str.replace(/\+/g, "-");
                    str = str.replace(/\//g, "_");
                    str = str.replace(/=/g, "");

                    cb(null, str);
                }
            });
        };


    tryAgain();
};

LevelDBDatabank.prototype.disconnect = function(callback) {
    var bank = this;

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }

    bank.handle.close(function(err) {
        if(err) {
            callback(err);
        } else {
            bank.handle = null;

            if (bank.mktmp) {
                leveldown.destroy(bank.file, callback);
            } else {
                callback(null);
            }
        }
    });
};

LevelDBDatabank.prototype.create = function(type, id, value, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }

    bank.handle.get(key, function(err, results) {
        if (err) {
            if (err.name === 'NotFoundError') {
                bank.handle.put(key, value, function(err) {
                    if (err) {
                        callback(err, null);
                    } else {
                        callback(null, value);
                    }
                });
            } else {
                callback(err, null);
            }
        } else {
            callback(new AlreadyExistsError(type, id), null);
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
            if (err.name === 'NotFoundError') {
                callback(new NoSuchThingError(type, id), null);
            } else {
                callback(err, null);
            }
        } else {
            callback(null, value);
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

    bank.handle.get(key, function(err, results) {
        if (err) {
            if (err.name === 'NotFoundError') {
                callback(new NoSuchThingError(type, id), null);
            } else {
                callback(err, null);
            }
        } else {
            bank.handle.put(key, value, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, value);
                }
            });
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

    bank.handle.get(key, function(err, results) {
        if (err) {
            if (err.name === 'NotFoundError') {
                callback(new NoSuchThingError(type, id), null);
            } else {
                callback(err, null);
            }
        } else {
            bank.handle.del(key, {}, callback);
        }
    });
};

LevelDBDatabank.prototype.search = function(type, criteria, onResult, onCompletion) {
    var bank = this;

    bank.scan(type, function(value) {
        if (bank.matchesCriteria(value, criteria)) {
            onResult(value);
        }
    }, onCompletion);
};

LevelDBDatabank.prototype.scan = function(type, onResult, onCompletion) {
    var bank = this;

    if (!bank.handle) {
        onCompletion(new NotConnectedError());
        return;
    }

    var dataStream = bank.handle.createReadStream();

    dataStream.on('data', function(data) {
        if (data.key.substr(0, type.length + 1) === type + ":") {
            onResult(data.value);
        }
    });
    dataStream.on('error', function(err) {
        onCompletion(err);
        dataStream.destroy();
    });
    dataStream.on('end', function() {
        onCompletion(null);
    });
};

module.exports = LevelDBDatabank;
