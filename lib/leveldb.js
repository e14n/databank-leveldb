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

LevelDBDatabank.prototype.makeTempDir = function(callback) {
    var bank = this,
        tmp = os.tmpDir(),
        tryAgain = function() {
            randomString(16, function(err, rs) {
                var dirname;
                if (err) {
                    callback(err);
                } else {
                    dirname = path.join(tmp, rs);
                    fs.stat(dirname, function(err, stats) {
		        if (err && err.code == 'ENOENT') {
                            bank.dir = dirname;
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

    bank.handle = null;

    if (bank.mktmp) {
        leveldb.destroy(bank.file, {}, callback);
    } else {
        callback(null);
    }
};

LevelDBDatabank.prototype.create = function(type, id, value, callback) {
    var bank = this,
        key = LevelDBDatabank.toKey(type, id);

    if (!bank.handle) {
        callback(new NotConnectedError(), null);
        return;
    }
    
    bank.handle.get(key, function(err, value) {
        if (value) {
            callback(new AlreadyExistsError(type, id), null);
        } else {
            bank.handle.put(key, JSON.stringify(value), function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, value);
                }
            });
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

LevelDBDatabank.prototype.search = function(type, criteria, onResult, onCompletion) {
    var bank = this;

    if (!bank.handle) {
        onCompletion(new NotConnectedError());
        return;
    }

    bank.handle.iterator(null, function(err, it) {
        var seenErr = null,
            onItem = function(err, key, value) {
                var obj;
                if (seenErr) {
                    return;
                }
                if (err) {
                    seenErr = err;
                    return;
                }
                if (key.substr(0, type.length + 1) === type + ":") {
                    obj = JSON.parse(value);
                    if (bank.matchesCriteria(obj, criteria)) {
                        onResult(obj);
                    }
                }
            },
            onFinish = function() {
                if (seenErr) {
                    onCompletion(seenErr);
                } else {
                    onCompletion(null);
                }
            };

        if (err) {
            onCompletion(err);
            return;
        }
        it.forRange(null, null, onItem, onFinish);
    });
};

module.exports = LevelDBDatabank;
