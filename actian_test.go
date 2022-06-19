// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"
	"time"
)

var (
	acdrv  = flag.String("acdriver", "{Actian AC}", "actian(ingres/avalanche driver name")
	acsrv  = flag.String("acserver", "", "actian(ingres/avalanche server name")
	acdb   = flag.String("acdb", "iidbdb", "actian(ingres/avalanche) database name")
	acuser = flag.String("acuser", "", "actian user name")
	acpass = flag.String("acpass", "", "actian password")
)

func actianConnect() (db *sql.DB, stmtCount int, err error) {
	// from https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-configuration-connection-parameters.html
	conn := fmt.Sprintf("driver=%s;server=%s;database=%s;uid=%s;pwd=%s;",
		*acdrv, *acsrv, *acdb, *acuser, *acpass)
	fmt.Println("Connection String ->", conn)
	db, err = sql.Open("odbc", conn)
	fmt.Println("sql.Open ->", err)
	if err != nil {
		return nil, 0, err
	}
	fmt.Println(err)
	stats := db.Driver().(*Driver).Stats
	return db, stats.StmtCount, nil
}

func TestActianTime(t *testing.T) {
	db, sc, err := actianConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB(t, db, sc, sc)
	fmt.Println("Connected")
	db.Exec("drop table temp")
	exec(t, db, "create table temp(id int not null auto_increment primary key, time time)")
	now := time.Now()
	// SQL_TIME_STRUCT only supports hours, minutes and seconds
	now = time.Date(1, time.January, 1, now.Hour(), now.Minute(), now.Second(), 0, time.Local)
	_, err = db.Exec("insert into temp (time) values(?)", now)
	if err != nil {
		t.Fatal(err)
	}

	var ret time.Time
	if err := db.QueryRow("select time from temp where id = ?", 1).Scan(&ret); err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret, now)
	if ret != now {
		t.Fatalf("unexpected return value: want=%v, is=%v", now, ret)
	}

	exec(t, db, "drop table temp")
}

func TestActianTransactions(t *testing.T) {
	db, sc, err := actianConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB(t, db, sc, sc)

	db.Exec("drop table txn_tst")
	exec(t, db, "create table txn_tst (name varchar(20))")

	var was, is int
	err = db.QueryRow("select count(*) from txn_tst").Scan(&was)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("insert into txn_tst (name) values ('tx1')")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRow("select count(*) from txn_tst").Scan(&is)
	if err != nil {
		t.Fatal(err)
	}
	if was+1 != is {
		t.Fatalf("is(%d) should be 1 more then was(%d)", is, was)
	}
	was = was + 1
	ch := make(chan error)
	go func() {
		// this will block until our transaction is finished
		err = db.QueryRow("select count(*) from txn_tst").Scan(&is)
		if err != nil {
			ch <- err
		}
		if was+1 != is {
			ch <- fmt.Errorf("is(%d) should be 1 more then was(%d)", is, was)
		}
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_, err = tx.Exec("insert into txn_tst (name) values ('tx2')")
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	err = <-ch
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow("select count(*) from txn_tst").Scan(&is)
	if err != nil {
		t.Fatal(err)
	}
	if was+1 != is {
		t.Fatalf("is(%d) should be 1 more then was(%d)", is, was)
	}

	was = is
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("insert into txn_tst (name) values ('tx3')")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRow("select count(*) from txn_tst").Scan(&is)
	if err != nil {
		t.Fatal(err)
	}
	if was+1 != is {
		t.Fatalf("is(%d) should be 1 more then was(%d)", is, was)
	}
	tx.Rollback()
	err = db.QueryRow("select count(*) from txn_tst").Scan(&is)
	if err != nil {
		t.Fatal(err)
	}
	if was != is {
		t.Fatalf("is(%d) should be equal to was(%d)", is, was)
	}

	exec(t, db, "drop table txn_tst")
}
