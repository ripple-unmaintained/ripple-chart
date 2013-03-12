#!/usr/bin/node
// Served at: /CUR/trades.json?since=123
// Served at: /CUR/orderbook.json
//
// TODO:
// - Finding min ledger is broken
// - Serve web pages
//

var mysql           = require('mysql');
var async           = require('async');
var Amount          = require("ripple-lib").Amount;
var Currency        = require("ripple-lib").Currency;
var Remote          = require("ripple-lib").Remote;
var UInt160         = require("ripple-lib").UInt160;

var Range           = require('./range').Range;

var mysql_config    = require("./config").mysql_config;
var config          = require("./config").config;
var markets         = require("./config").markets;
var rippled_config  = require("./config").rippled_config;
var btc_gateways    = require("./config").btc_gateways;

var remote;
var sources = [];

// ledger: { CUR_ACCOUNT: true }
// CUR_ACCOUNT: <ledger>
var wanted  = {};
var validated_ledgers;

var wanted_ledger = function (ledger) {
  return Object.keys(wanted[ledger] || {}).map(function(k) {
      var parts = k.split(":");
      
      return {
        currency: parts[0],
        address: parts[1]
      };
    });
};

var wanted_get = function (source) {
  var key = source.currency + ":" + source.address;

  return wanted[key];
};

var wanted_set = function (source, ledger_next, ledger_old) {
//  console.log("before", JSON.stringify(wanted, undefined, 2));
  var key = source.currency + ":" + source.address;

  if (ledger_old && wanted[ledger_old]) {
    delete wanted[ledger_old][key];

    if (!wanted[ledger_old].length)
      delete wanted[ledger_old];

    delete wanted[key];
  }

  if (ledger_next)
  {
    wanted[key] = ledger_next;

    wanted[ledger_next] = wanted[ledger_next] || {};
    wanted[ledger_next][key]  = true;
  }
//  console.log("after", JSON.stringify(wanted, undefined, 2));
};

var wanted_min = function () {
  var ledger_min  = undefined;

  Object.keys(wanted).forEach(function (ledger_next) {
      var ledger  = Number(ledger_next);

      if (ledger_next === String(ledger)
        && (!ledger_min || ledger < ledger_min))
        ledger_min  = ledger;
    });

  return ledger_min;
};

// callback(err)
var setup_sources = function (callback)
{
  // Make an array of source objects.
  Object.keys(markets).forEach(function (currency) {
      Object.keys(markets[currency]).forEach(function (address) {
          sources.push({
              currency: currency,
              address: address
            });
        });
    });

  console.log("sources: ", JSON.stringify(sources, undefined, 2));

  // For each source, discover its processed range.
  db_perform(function (err, conn, done) {
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

                wanted_set(source, Number(ledger_next));
              }
    
              callback(!err);
            });

        }, function (result) {
          console.log("wanted: ", JSON.stringify(wanted, undefined, 2));

          done();
          callback(!result)
        });
    });
};

var insert_ledger = function (conn, records, done) {
  // console.log("source: ", JSON.stringify(source));
  if (records.length) 
  {
    conn.query("INSERT INTO Transactions (Hash, Currency, LedgerTime, LedgerIndex, Price, Amount) VALUES ?",
        records.map(function (r) {
          return [[ r.Hash, r.Currency, r.LedgerTime, r.LedgerIndex, r.Price, r.Amount ]];
        }),
      function (err, results) {
        if (err && 'ER_DUP_ENTRY' === err.code)
          err = null;

        if (err)
          console.log("Insert: %s", JSON.stringify({ err: err, results: results}, undefined, 2));

        done(err);
      });
  }
  else {
    done();
  }
};

// done(err)
var process_ledger = function (conn, ledger_index, done) {
//  console.log("!!!! process_ledger: ", ledger_index);

  remote.request_ledger(undefined, { transactions: true, expand: true })
    .ledger_index(ledger_index)
    .on('error', function (m) {
        console.log("request error: ", JSON.stringify(m, undefined, 2));

        done(m);
      })
    .on('success', function (m) {
        var   ledger_out  = false;
        var   ledger      = m.ledger;
        var   records     = [];

        // Rewrite depricated format.
        ledger.transactions.forEach(function (t) {
          if (t.metaData)
          {
            t.meta  = t.metaData;
            delete t.metaData;
          }
        });

        // Filter to relevant transactions.
        ledger.transactions = ledger.transactions.filter(function (t) {
            return (t.TransactionType === 'OfferCreate'
                    || t.TransactionType === 'Payment')
                    && t.meta.TransactionResult === 'tesSUCCESS';
          });

        // Sort transaction into processed order.
        ledger.transactions.sort(function (a, b) {
            return a.meta.TransactionIndex - b.meta.TransactionIndex;
          });

        // Process each transaction.
        ledger.transactions.forEach(function (t) {
          var   trades  = [];
          var   b_asking;         // True if selling btc. Price going down.

          t.meta.AffectedNodes.forEach(function (n) {
              var base;
              
              if (n.ModifiedNode)
                base  = n.ModifiedNode;
              else if (n.DeletedNode)
                base  = n.DeletedNode;

              if (base
                && base.LedgerEntryType === 'Offer'
                && base.PreviousFields
                && 'TakerGets' in base.PreviousFields
                && 'TakerPays' in base.PreviousFields) {
                var pf          = base.PreviousFields;
                var ff          = base.FinalFields;

                var taker_got   = Amount.from_json(pf.TakerGets).subtract(Amount.from_json(ff.TakerGets));
                var taker_paid  = Amount.from_json(pf.TakerPays).subtract(Amount.from_json(ff.TakerPays));
                var b_got_btc   = taker_got.currency().to_human() == 'BTC';
                var b_paid_btc  = taker_paid.currency().to_human() == 'BTC';

                if (b_got_btc != b_paid_btc && taker_got.is_positive() && taker_paid.is_positive())
                {
                  b_asking  = b_paid_btc;

                  // Prefer taker_paid to be BTC.
                  if (b_got_btc) {
                    var tg  = taker_got;
                    var tp  = taker_paid;

                    taker_got   = tp;
                    taker_paid  = tg;
                  }

                  var paid_issuer    = taker_paid.issuer().to_json();
                  var got_currency  = taker_got.currency().to_human();
                  var got_issuer    = taker_got.issuer().to_json();

                  if (btc_gateways[paid_issuer]   // Have a btc we care about
                    && (taker_got.is_native()
                      || (markets[got_currency] && markets[got_currency][got_issuer]))) { // Have an counter currency we care about.

                    if (!ledger_out)
                    {
                      ledger_out = true;
                      console.log("LEDGER: ", ledger_index);
                      // console.log("LEDGER: ", JSON.stringify(ledger, undefined, 2));
                      // console.log("t: ", JSON.stringify(t, undefined, 2));
                    }

                    var record = {
                      Hash:         t.hash,
                      Currency:     got_currency,
                      LedgerTime:   ledger.close_time,
                      LedgerIndex:  ledger.ledger_index,
                      Price:        taker_got.divide(taker_paid).to_human({
                                        precision: 4,
                                        group_sep: false,
                                      }),
                      Amount:       taker_paid.to_human({
                                        precision: 8,
                                        group_sep: false,
                                      }),
                    };

                    console.log("Record: ", JSON.stringify(record));

                    trades.push(record);

                    // console.log("TRADE: %s for %s", taker_paid.to_human_full(), taker_got.to_human_full());

                    // console.log("Node: ", JSON.stringify(n, undefined, 2));
                  }
                }
              }
            });

          if (trades.length)
          {
            trades.sort(function (a,b) {
                return b_asking
                  ? b.Price-a.Price             // Selling BTC, better, higher prices first.
                  : a.Price-b.Price;            // Buying BTC, better, lower, prices first.
              });

            records = records.concat(trades);
          }
        });

        insert_ledger(conn, records, 
          function (err) {
            if (!err)
            {
              // Advance to next ledger.
              wanted_ledger(ledger_index).forEach(function (s) {
                  // console.log("GET: %s/%s", JSON.stringify(wanted_get(s)), JSON.stringify(ledger_index));

                  //console.log("DOING: ", JSON.stringify(s, undefined, 2));

                  wanted_set(s, Number(ledger_index)+1, Number(ledger_index));
                });
            }

            done(err);
          });
      })
    .request();
};

// Process what we can based on validated_ledgers
// We have the genesis ledger.
var process_range = function (conn, validated_ledgers, done)
{
  // Do wanted strictly in order.
  // XXX Later add a list of ledgers to skip in the config.
  
  var ledger_index  = wanted_min();
//console.log("MIN: ", ledger_index);

  if (ledger_index && validated_ledgers.has_member(ledger_index)) {
    process_ledger(conn, ledger_index, function (err) {
        if (err) {
          done(err);
        }
        else {
          process_range(conn, validated_ledgers, done);
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

  // Skip processing, if rippled does not yet contain the effective genesis ledger.
  if (validated_ledgers.has_member(Number(config.genesis_ledger))) {
    if (self.processing) {
      console.log("process_validated: processing in progress: deferring: %s", str_validated_ledgers);

    }
    else {
      console.log("process_validated: %s", str_validated_ledgers);

      self.processing = true;
      db_perform(function (err, conn, done) {
          process_range(conn, validated_ledgers, function (err) {
              self.processing = false;

              console.log("process_validated: concluded");
              done(err);
            });
        });
    }
  }
  else {
    console.log("process_validated: waiting for genesis ledger: %s/%s", config.genesis_ledger, str_validated_ledgers);
  }
};

// callback(err, conn, done);
// done(err);
// done(err);
var db_perform = function (callback, done) {
  // console.log("mysql_config: ", JSON.stringify(mysql_config.user, undefined, 2));

  var conn  = mysql.createConnection(mysql_config);
    conn
      .connect(function (err) {
        // console.log("Conncted: %s", JSON.stringify(err, undefined, 2));

        // Connected, call the user function.
        callback(err, conn, function (err) {
            // Disconnect.
            conn.end();

            // Call done.
            !done || done(err);
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
        !done || done(err);
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
  db_perform(function (err, conn, done) {
    async.waterfall([
        function (callback) {
          callback(err);
        },
        function (callback) {
          var sql_drop_processed    = "DROP TABLE IF EXISTS Processed;";

          conn.query(sql_drop_processed, function (err, results) {
              // console.log("drop_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
          var sql_drop_transactions = "DROP TABLE IF EXISTS Transactions;";

          conn.query(sql_drop_transactions, function (err, results) {
              // console.log("drop_transactions: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
            var sql_create_processed =  // Range of ledgers processed.
                "CREATE TABLE Processed ("
              + "  Currency     CHARACTER(3),"
              + "  Account      CHARACTER(35),"
              + "  Done         TEXT,"
              + "  PRIMARY KEY (Currency, Account)"
              + ") TYPE = " + config.table_type + ";";

          conn.query(sql_create_processed, function (err, results) {
              // console.log("create_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
            var sql_create_transactions =
                "CREATE TABLE Transactions ("
              + "  Currency     CHARACTER(3),"
              + "  LedgerTime   INTEGER UNSIGNED,"               // ledger_time
              + "  LedgerIndex  INTEGER UNSIGNED,"               // ledger_index
              + "  Hash         CHARACTER(32) UNIQUE,"
              + "  Price        VARCHAR(32),"
              + "  Amount       VARCHAR(32),"
              + "  Tid          INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,"
              + ""
              + "  UNIQUE name (Currency, Tid)"
              + ") TYPE = " + config.table_type + ";";

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
        done();
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

// m.validated_ledgers = '32570-335732';
              if ('validated_ledgers' in m) {
                process_validated(m.validated_ledgers);
              }
            })
          .on('error', function (e) {
              console.log('Remote error: ', e);
              throw e;
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
