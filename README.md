databank-leveldb
================

This is the LevelDB driver for Databank.

License
-------

Copyright 2012, StatusNet Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

> http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Usage
-----

To create a LevelDB databank, use the `Databank.get()` method:

    var Databank = require('databank').Databank;
    
    var db = Databank.get('leveldb', {});

The driver takes the following parameters:

* `file`: the LevelDB directory. Defaults to `/tmp/databank-leveldb`.
* `schema`: the database schema, as described in the Databank README.
* `mktmp`: If set, ignore the `file` argument and make a new,
  temporary DB that will be deleted on disconnection. Good for caches or
  unit tests. Defaults to `false`.

See the main databank package for info on its interface.

Under the covers
----------------

Objects, arrays and numbers are stored as JSON-encoded strings in the
LevelDB database.

Keys in the database have the form "type:id". So a "person" with id
"evanp" is at "person:evanp".
