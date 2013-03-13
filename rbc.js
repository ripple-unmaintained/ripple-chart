#!/usr/bin/node
//
// Ripple server for bitcoincharts.com as per http://bitcoincharts.com/about/exchanges/
//
// Served at: /CUR/trades.json?since=123
// Served at: /CUR/orderbook.json
//
// TODO:
// - Finding min ledger is broken
//

var async           = require('async');
var http            = require("http");
var mysql           = require('mysql');
var url             = require("url");
var Amount          = require("ripple-lib").Amount;
var Currency        = require("ripple-lib").Currency;
var Remote          = require("ripple-lib").Remote;
var UInt160         = require("ripple-lib").UInt160;

var Range           = require('./range').Range;

var btc_gateways    = require("./config").btc_gateways;
var config          = require("./config").config;
var httpd_config    = require("./config").httpd_config;
var markets         = require("./config").markets;
var mysql_config    = require("./config").mysql_config;
var rippled_config  = require("./config").rippled_config;

var remote;
var sources = [];

var processed = {
};

// Information to serve on the status page.
var info    = {
  btc_gateways: btc_gateways,
  markets:      markets,
};

// A cache of the asks and bids for all order books cared about.
// The web server services this structure.
// This structure is updated on each ledger_close.
// CUR : { "asks" : [[price, amount], ...], "bids" : [...] }
var books = {};

// Return the minimum next ledger to process.
var leger_next = function () {
  var   ledger_index;

  Object.keys(markets).forEach(function (currency) {
      Object.keys(markets[currency]).forEach(function (address) {
          var i = processed[currency][address].is_empty()
                    ? config.genesis_ledger
                    : processed[currency][address].last()+1;

          if (!ledger_index || i < ledger_index) {
            ledger_index  = i; 
          }
        });
    });

//console.log("leger_next: %s", JSON.stringify(ledger_index));
  return ledger_index;
};

// Return an array of sources that want a ledger_index next.
var wanted_ledger = function (ledger_index) {
  var _wanted  = [];

  Object.keys(markets).forEach(function (currency) {
      Object.keys(markets[currency]).forEach(function (address) {
          var _next = processed[currency][address].is_empty()
                        ? config.genesis_ledger
                        : processed[currency][address].last()+1;

          if (_next === ledger_index) {
            _wanted.push({
                currency: currency,
                issuer:   address
              });
          }
        });
    });

//console.log("wanted: %s", JSON.stringify(_wanted));
  return _wanted;
};

var set_processed = function (source, ledger_index) {
// console.log("processed% %s > '%s'", JSON.stringify(source), JSON.stringify(processed, undefined, 2));
    var   _processed  = processed[source.currency][source.issuer].insert(ledger_index);

    processed[source.currency][source.issuer] = _processed
//console.log("_processed% %s > '%s'", JSON.stringify(source), _processed.to_string());
};

var replace_processed = function (conn, done) {
  var _rows  = [];

  Object.keys(markets).forEach(function (currency) {
      Object.keys(markets[currency]).forEach(function (address) {
            _rows.push(
              [
                currency,
                address,
                processed[currency][address].to_string(),
              ]);
        });
    });

// console.log("REPLACE: ", JSON.stringify(_rows, undefined, 2));

  conn.query("REPLACE Processed (Currency, Account, Done) VALUES ?",
    [_rows],
    function (err, results) {
      if (err)
      {
        console.log("ERR: REPLACE: %s", JSON.stringify({ err: err, results: results}, undefined, 2));
      }

      done(err);
    });
};

var setup_tables = function () {
  // Make an array of source objects.
  Object.keys(markets).forEach(function (currency) {
      processed[currency] = {};

      Object.keys(markets[currency]).forEach(function (address) {
          sources.push({
              currency: currency,
              issuer:   address
            });

          processed[currency][address] = new Range;
        });

      books[currency] = { asks: [], bids: [] };
    });
};

// callback(err)
var setup_sources = function (callback)
{
  // console.log("sources: ", JSON.stringify(sources, undefined, 2));

  // For each source, discover its processed range.
  db_perform(function (err, conn, done) {
      if (err)
        throw err;

      conn.query("SELECT * FROM Processed",
        function (err, results) {
          // console.log("Range: %s", JSON.stringify({ err: err, results: results}, undefined, 2));

          if (!err) {
            results.map(function (row) {
                processed[row.Currency][row.Account]  = Range.from_string(row.Done);
              });
          }

          callback(!err);
        });
    });
};

var insert_ledger = function (conn, records, done) {
  // console.log("source: ", JSON.stringify(records));
  if (records.length) 
  {
    conn.query("INSERT Trades (Hash, Currency, LedgerTime, LedgerIndex, Price, Amount) VALUES ?",
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

                    if (!ledger_out)
                    {
                      ledger_out = true;
                      // console.log("LEDGER: ", ledger_index);
                      // console.log("LEDGER: ", JSON.stringify(ledger, undefined, 2));
                      // console.log("t: ", JSON.stringify(t, undefined, 2));
                    }

                    // console.log("Record: ", JSON.stringify(record));

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
              // Mark ledger as processed for each source that needs it.
              wanted_ledger(ledger_index).forEach(function (s) {
                  set_processed(s, ledger_index);
                });

              // replace_processed(conn, done);
            }
            else
            {
              // done(err);
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
  
  var ledger_index  = leger_next();
// console.log("NEXT: ", ledger_index);
// console.log("validated_ledgers: %s / %s", validated_ledgers.to_string(), validated_ledgers.is_member(ledger_index));

  if (ledger_index && validated_ledgers.is_member(ledger_index)) {
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
    // Save progress to db.
    replace_processed(conn, done);
  }
};

var process_validated = function (str_validated_ledgers)
{
  var self              = this;
  var validated_ledgers = Range.from_string(str_validated_ledgers);

  // Skip processing, if rippled does not yet contain the effective genesis ledger.
  if (validated_ledgers.is_member(config.genesis_ledger)) {
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

var process_books = function (ledger_index) {
  var books_new = {};

  Object.keys(markets).forEach(function (_currency) {
      books_new[_currency] = { asks: [], bids: [] };
    });

  // console.log("MARKETS %s", JSON.stringify(Object.keys(markets)));
  async.each(Object.keys(markets), function (_currency, market_callback) {
      var _pairs  = [];

      Object.keys(markets[_currency]).forEach(function (cur_address) {
        var _cur  = {
          currency: _currency,
          issuer:   cur_address,
        };

        Object.keys(btc_gateways).forEach(function (btc_address) {
            var _btc  = {
              currency: 'BTC',
              issuer:   btc_address,
            };

            _pairs.push({ gets: _btc, pays: _cur }, { gets: _cur, pays: _btc });
          });
      });

      // console.log("MARKET> %s", _currency);
      async.some(_pairs, function (_pair, callback) {
          remote.request_book_offers(_pair.gets, _pair.pays)
            .ledger_index(ledger_index)
            .on('success', function (m) {
                // console.log("BOOK: ", JSON.stringify(m, undefined, 2));

                var _side = _pair.gets.currency === 'BTC' ? 'asks' : 'bids';

                books_new[_currency][_side] = books_new[_currency][_side].concat(
                  m.offers
                    .map(function (o) {
                        // console.log("OFFER: ", JSON.stringify(o, undefined, 2));

                        var _taker_gets = Amount.from_json('taker_gets_funded' in o ? o.taker_gets_funded : o.TakerGets);
                        var _taker_pays = Amount.from_json('taker_pays_funded' in o ? o.taker_pays_funded : o.TakerPays);

                        if (_side === 'asks') {
                          var _tg = _taker_gets;
                          var _tp = _taker_pays;

                          _taker_gets = _tp;
                          _taker_pays = _tg;
                        }

                        var _price      = _taker_gets.divide(_taker_pays).to_human({
                                            precision: 8,
                                            group_sep: false,
                                          });
                        var _amount     = _taker_pays.to_human({
                                            precision: 8,
                                            group_sep: false,
                                          });

                        return Number(_price) ? [_price, _amount] : null;
                      })
                    .filter(function (o) { return o; })
                  );

                callback();
              })
            .on('error', function (m) {
                console.log("ERROR BOOK: ", JSON.stringify(m, undefined, 2));

                callback(m);
              })
            .request();
            
        }, function (err) {
          if (err) {
            console.log("process_books: error: ledger_index: %s market: %s", ledger_index, _currency);
          }
          else {
            // console.log("REVISE BOOK: %s %s", _currency, JSON.stringify(books_new[_currency], undefined, 2));
            books_new[_currency].bids.sort(function (a, b) { return Number(b[0])-Number(a[0]); });
            books_new[_currency].asks.sort(function (a, b) { return Number(a[0])-Number(b[0]); });

            books[_currency] = books_new[_currency];
          }

          market_callback(err);
        });

      }, function (err) {
      });
};

var do_reset = function () {
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
          var sql_drop_transactions = "DROP TABLE IF EXISTS Trades;";

          conn.query(sql_drop_transactions, function (err, results) {
              // console.log("drop_transactions: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
            var sql_create_processed = 
                "CREATE TABLE Processed ("
              + "  Currency     CHARACTER(3),"
              + "  Account      CHARACTER(35),"
              + "  Done         TEXT,"                            // Range of ledgers processed.
              + "  PRIMARY KEY (Currency, Account)"
              + ") TYPE = " + config.table_type + ";";

          conn.query(sql_create_processed, function (err, results) {
              // console.log("create_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
            var sql_create_transactions =
                "CREATE TABLE Trades ("
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

var do_httpd = function () {
  var self    = this;

  var server  = http.createServer(function (req, res) {
      // console.log("CONNECT");
      // var input = "";

      req.setEncoding();

      req.on('data', function (buffer) {
          // console.log("DATA: %s", buffer);
          // input = input + buffer;
        });

      req.on('end', function () {
          // console.log("END");

          var _parsed = url.parse(req.url, true);
          var _m;

          // console.log("URL: %s", req.url);
          // console.log("HEADERS: %s", JSON.stringify(req.headers, undefined, 2));
          // console.log("INPUT: %s:", JSON.stringify(input));
          // console.log("_parsed: %s", JSON.stringify(_parsed, undefined, 2));

          if (_parsed.pathname === "/") {
            res.statusCode = 200;
            res.end(JSON.stringify({
                processing:   self.processing,
                info:         info
              }, undefined, 2));

          }
          else if (_m = _parsed.pathname.match(/^\/(...)\/trades.json$/)) {
            var   _market   = _m[1] && _m[1] in markets && _m[1];
            var   _since    = _parsed.query.since;

            if (!_market) {
              res.statusCode = 204;
              res.end(JSON.stringify({
                  pathname: _parsed.pathname,
                  message:  'bad market',
                  market:   _market
                }));
            }
            else if (!_since) {
              res.statusCode = 204;
              res.end(JSON.stringify({
                  trades: true,
                  since: 'missing'
                }));
            }
            else {
              db_perform(function (err, conn, done) {
                  if (err) {
                    done(err);
                  }
                  else {
                    conn.query("SELECT * FROM Trades WHERE Currency=? AND Tid >= ? ORDER BY Tid ASC LIMIT ?",
                      [_market, _since, config.trade_limit],
                      function (err, results) {
                          if (err) {
                            console.log("err: %s", JSON.stringify(err, undefined, 2));
                          }
                          else {
//                          console.log("results: %s", JSON.stringify(results, undefined, 2));

                            res.statusCode = 200;
                            res.end(JSON.stringify(
                                results.map(function (r) {
                                    return {
                                        date:   r.LedgerTime+946684800,
                                        price:  r.Price, 
                                        amount: r.Amount,
                                        tid:    r.Tid
                                      };
                                  }), undefined, 1));
                          }

                          done(err);
                        });
                  }
                },
                function (err) {
                  if (err) {
                    res.statusCode = 500;
                    res.end(JSON.stringify({
                        trades: true,
                        market: _market,
                        since: _since
                      }));
                  }
                });
            }
          }
          else if (_m = _parsed.pathname.match(/^\/(...)\/orderbook.json$/)) {
            var   _market   = _m[1];
            var   _since    = _parsed.query.since;

            if (!(_market in markets)) {
              res.statusCode = 204;
              res.end(JSON.stringify({
                  pathname: _parsed.pathname,
                  message:  'bad market',
                  market:   _market
                }));
            }
            else {
              res.statusCode = 200;
              res.end(JSON.stringify(books[_market], undefined, 1));
            }
          }
          else
          {
            res.statusCode = 404;
            res.end(JSON.stringify({
                message: 'not found',
                parsed: JSON.stringify(_parsed, undefined, 2)
              }));
          }
        });

//      req.on('close', function () {
//          console.log("CLOSE");
//        });
    });

  server.listen(httpd_config.port, httpd_config.ip, undefined,
    function () {
      console.log("Listening at: %s:%s", httpd_config.ip, httpd_config.port);
    });
  
};

var do_perform = function () {
  var self = this;
  
  setup_sources(function () {
      remote  =
        Remote
          .from_config(rippled_config)
          .on('ledger_closed', function (m) {
              // console.log("ledger_closed: ", JSON.stringify(m, undefined, 2));

              info.ledger = m;

              if ('validated_ledgers' in m) {
                process_validated(m.validated_ledgers);

                process_books(m.ledger_index);
              }
            })
          .on('error', function (e) {
              console.log('Remote error: ', e);
              throw e;
            })

          .connect();
    });
};

var do_usage = function () {
  console.log(
      "Usage: %s\n"
    + " reset - initialize dbs\n"
    + " perform - serve and update the database\n",
    process.argv[1]);
};

var main = function () {
  if (3 === process.argv.length && "reset" === process.argv[2])
  {
    do_reset();
  }
  else if (3 === process.argv.length && "perform" === process.argv[2])
  {
    setup_tables();

    do_httpd();
    do_perform();
  }
  else
  {
    do_usage();
  }
};

main();

// vim:sw=2:sts=2:ts=8:et
