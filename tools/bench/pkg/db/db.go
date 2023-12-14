/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package db

import (
	"errors"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
)

const (
	Bucket   = "bucket"
	ObjSize  = "obj_size"
	ObjCount = "obj_count"
	ObjTotal = "obj_total"
	Started  = "started"
	Parallel = "parallel"
)

var _dbBucket = []byte("chorus")

type DB struct {
	b *bolt.DB
}

func New(path string, readonly bool) (*DB, error) {
	mode := 0600
	if readonly {
		mode = 0666
	}
	bdb, err := bolt.Open(path, os.FileMode(mode), &bolt.Options{ReadOnly: readonly})
	if err != nil {
		return nil, err
	}
	if !readonly {
		err = bdb.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket(_dbBucket)
			if errors.Is(err, bolt.ErrBucketExists) {
				return nil
			}
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	return &DB{b: bdb}, nil
}

func (db *DB) Get(s string) (res string, err error) {
	err = db.b.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(_dbBucket).Get([]byte(s))
		if b != nil {
			res = string(b)
		}
		return nil
	})
	return
}

func (db *DB) Put(k, v string) error {
	return db.b.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(_dbBucket).Put([]byte(k), []byte(v))
	})
}

func (db *DB) Del(k string) error {
	return db.b.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(_dbBucket).Delete([]byte(k))
	})
}

func (db *DB) GetInt(s string) (res int64, err error) {
	err = db.b.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(_dbBucket).Get([]byte(s))
		if b == nil {
			return nil
		}
		res, err = strconv.ParseInt(string(b), 10, 64)
		return err
	})
	return
}

func (db *DB) PutInt(k string, v int64) error {
	return db.b.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(_dbBucket).Put([]byte(k), []byte(strconv.FormatInt(v, 10)))
	})
}

func (db *DB) Close() error {
	return db.b.Close()
}
