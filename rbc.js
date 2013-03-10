#!/usr/bin/node

// Served at: /CUR/trades.json?since=123
// Served at: /CUR/orderbook.json

var mysql           = require('mysql');
var async           = require('async');
var Remote          = require("ripple-lib").Remote;

var Range           = require('./range').Range;

var connection      = require("./config").connection;
var config          = require("./config").config;
var markets         = require("./config").markets;
var rippled_config  = require("./config").rippled_config;

var remote;
var sources = [];

// ledger: [ sources ]
var wanted  = {};
var validated_ledgers;

// callback(err)
var setup_sources = function (callback)
{
  // Make an array of source objects.
  Object.keys(markets).map(function (currency) {
      Object.keys(markets[currency]).map(function (address) {
          sources.push({
              currency: currency,
              address: address
            });
        });
    });

  console.log("sources: ", JSON.stringify(sources, undefined, 2));

  // For each source, discover its processed range.
  db_perform(function (err, conn, disconnect) {
      if (err)
        throw err;

      async.every(sources, function (source, callback) {
          console.log("source: ", JSON.stringify(source));

          conn.query("SELECT Done FROM Processed WHERE Currency=? AND Account=?",
            [source.currency, source.account], 
            function (err, results) {
              console.log("Range: %s", JSON.stringify({ err: err, results: results}, undefined, 2));

              if (!err) {
                var ledger_next = results.length
                  ? results[1]
                  : config.genesis_ledger;

                wanted[ledger_next] = wanted[ledger_next] || [];
                wanted[ledger_next].push(source);
              }
    
              callback(!err);
            });

        }, function (result) {
          console.log("wanted: ", JSON.stringify(wanted, undefined, 2));

          disconnect();
          callback(!result)
        });
    });
};

var wanted_next = function () {
  var ledger_min  = undefined;

  Object.keys(wanted).map(function (ledger_next) {
      if (!ledger_min || ledger_next < ledger_min)
        ledger_min  = ledger_next;
    });

  return ledger_min;
};

// done(err)
var process_ledger = function (next, done) {
  console.log("process_ledger: ", next);

  done("failed");
};

// Process what we can based on validated_ledgers
// We have the genesis ledger.
var process_range = function (validated_ledgers, done)
{
  // Do wanted strictly in order.
  // XXX Later add a list of ledgers to skip in the config.
  
  var next  = wanted_next();

  if (next && validated_ledgers.has_member(next)) {
    process_ledger(next, function (err) {
        if (err) {
          done();
        }
        else {
          process_range(validated_ledgers, done);
        }
      });
  }
  else
  {
    done();
  }
};

var process_validated = function (str_validated_ledgers)
{
  var self              = this;
  var validated_ledgers = Range.from_string(str_validated_ledgers);

  if (validated_ledgers.has_member(config.genesis_ledger)) {
    console.log("process_validated: %s", str_validated_ledgers);
  
    if (!self.processing) {
      self.processing = true;
      process_range(validated_ledgers, function () {
          self.processing = false;

          console.log("process_validated: concluded");
        });
    }
  }
  else {
    console.log("No genesis ledger: %s", str_validated_ledgers);
  }
};

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
          + "  Currency     CHARACTER(3),"
          + "  Account      CHARACTER(35),"
          + "  Done         TEXT,"
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
          callback(err);
        },
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
  
  setup_sources(function () {
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
    });
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
