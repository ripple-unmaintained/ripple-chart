#!/usr/bin/node

// Served at: /CUR/trades.json?since=123
// Served at: /CUR/orderbook.json

var mysql           = require('mysql');
var async           = require('async');
var Remote          = require("ripple-lib").Remote;

var Range           = require('./range').Range;

var connection      = require("./config").connection;
var config          = require("./config").config;
var rippled_config  = require("./config").rippled_config;

var remote;

var process_validated = function (str_validated_ledgers)
{
  var validated_ledgers = Range.from_string(str_validated_ledgers);

  if (validated_ledgers.has_member(config.genesis_ledger)) {
    console.log("Processing: %s", str_validated_ledgers);
  
  }
  else {
    console.log("No genesis ledger: %s", str_validated_ledgers);
  }
}

// callback(err, conn, disconnect);
// disconnect();
var db_perform = function (callback) {
  // console.log("user: ", connection.user);

  var conn  = mysql.createConnection(connection);
    conn
      .connect(function (err) {
        console.log("Conncted: %s", JSON.stringify(err, undefined, 2));

        callback(err, conn, function () {
            conn.end();
          });
      });

    conn
      .on('error', function (err) {
        if (!err.fatal)
          return;

        if (err.code !== 'PROTOCOL_CONNECTION_LOST') {
          throw err;
        }

        console.log("*** disconnected.");
      });
};

var do_usage = function () {
  console.log(
      "Usage: %s\n"
    + " db_init - initialize dbs\n"
    + " perform- serve and update the database\n",
    + " status - show status\n",
    process.argv[1]);
};

var do_db_init = function () {
  var sql_drop_processed    = "DROP TABLE IF EXISTS Processed;";
  var sql_drop_transactions = "DROP TABLE IF EXISTS Transactions;";

  var sql_create_processed =  // Range of ledgers processed.
            "CREATE TABLE Processed ("
          + "  Currency       CHARACTER(3),"
          + "  Account        CHARACTER(35),"
          + "  PRIMARY KEY (Currency, Account)"
          + ") TYPE = " + config.table_type + ";";

  var sql_create_transactions =
            "CREATE TABLE Transactions ("
          + "  Currency     CHARACTER(3),"
          + "  LedgerTime   INTEGER UNSIGNED,"               // ledger_time
          + "  LedgerIndex  INTEGER UNSIGNED,"               // ledger_index
          + "  Price        VARCHAR(32),"
          + "  Amount       VARCHAR(32),"
          + "  Tid          INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,"
          + ""
          + "  UNIQUE name (Currency, Tid)"
          + ") TYPE = " + config.table_type + ";";

  db_perform(function (err, conn, disconnect) {
    async.waterfall([
        function (callback) {
          conn.query(sql_drop_processed, function (err, results) {
              // console.log("drop_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
          conn.query(sql_drop_transactions, function (err, results) {
              // console.log("drop_transactions: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
          conn.query(sql_create_processed, function (err, results) {
              // console.log("create_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
          conn.query(sql_create_transactions, function (err, results) {
              // console.log("create_transactions: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        }
      ], function (err, result) {
        if (err) {
          console.log("Error: ", JSON.stringify(err, undefined, 2));
        }
        else {
          console.log("Success");
        }
        disconnect();
      });
    });

};

var do_status = function () {
  console.log("status: not implemented.");

  var ledger_index = 2000;

  // Returns how far caught up each source is.
  return {
    USD: {
      'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B' : ledger_index,
    },
  };
};

var do_perform = function () {
  var self = this;
  
  remote  =
    Remote
      .from_config(rippled_config)
      .on('ledger_closed', function (m) {
          console.log("ledger_closed: ", JSON.stringify(m, undefined, 2));

          if ('validated_ledgers' in m) {
            process_validated(m.validated_ledgers);
          }
        })
      .connect();
};

var main = function () {
  if (3 === process.argv.length && "db_init" === process.argv[2])
  {
    do_db_init();
  }
  else if (3 === process.argv.length && "perform" === process.argv[2])
  {
    do_perform();
  }
  else if (3 === process.argv.length && "status" === process.argv[2])
  {
    do_status();
  }
  else
  {
    do_usage();
  }
};

main();

// vim:sw=2:sts=2:ts=8:et
