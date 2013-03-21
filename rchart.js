#!/usr/bin/node
//
// ripple-chart: rchart.js
//
// Ripple server for bitcoincharts.com as per http://bitcoincharts.com/about/exchanges/
//

var async           = require('async');
var EventEmitter    = require('events').EventEmitter;
var http            = require("http");
var mysql           = require('mysql');
var url             = require("url");
var Amount          = require("ripple-lib").Amount;
var Currency        = require("ripple-lib").Currency;
var Remote          = require("ripple-lib").Remote;
var UInt160         = require("ripple-lib").UInt160;

var Range           = require('./range').Range;

var config          = require("./config").config;
var httpd_config    = require("./config").httpd_config;
var markets         = require("./config").markets;
var mysql_config    = require("./config").mysql_config;
var rippled_config  = require("./config").rippled_config;

var remote;
var currencies      = Object.keys(markets);
var sources         = [];

var pairs   = [
  // [ 'CCY1', CCY2' ], ...   // 'CCY1' < 'CCY2'
];

var processed = {};

// Information to serve on the status page.
var info    = {
  markets:      markets,
};

// A cache of the asks and bids for all order books cared about.
// The web server serves this structure.
// Constraint: CCY1 != CCY2
var books = {
  // CCY1CCY2: {
  //   ledger_index: index,      // The version text presents.
  //   text: string              // The order book as text.
  //   processing: EventEmitter, // Build a new version of text. Listen here for announcement.
  //   asks: array,
  //   bids: array,
  // },
};

// Takes an array of arrays.
var permute = function (_arrays) {
// console.log("permute: ", JSON.stringify(_arrays));
  if (!_arrays.length) {
    return [];
  }
  else if (_arrays.length === 1) {
    return _arrays[0].map(function (_e) { return [_e]; });
  }
  else {
    var _tail_permuted = permute(_arrays.slice(1));

    return [].concat.apply([],
      _arrays[0].map(function (_e) {
          return _tail_permuted.map(function (_t) {
              return [_e].concat(_t);
            });
        }));
  }
};

// Return the minimum next ledger to process.
var leger_next = function () {
  var   ledger_index;

// console.log("leger_next: processed: %s", JSON.stringify(processed, undefined, 2));
  sources.forEach(function (_source) {
      var i = processed[_source.currency][_source.issuer].is_empty()
                ? config.genesis_ledger
                : processed[_source.currency][_source.issuer].last()+1;

      if (!ledger_index || i < ledger_index) {
        ledger_index  = i; 
      }
    });

//console.log("leger_next: %s", JSON.stringify(ledger_index));
  return ledger_index;
};

// Return an array of sources that want a ledger_index next.
var wanted_ledger = function (ledger_index) {
  var _wanted  = [];

  return sources.filter(function(_source) {
      var _next = processed[_source.currency][_source.issuer].is_empty()
                    ? config.genesis_ledger
                    : processed[_source.currency][_source.issuer].last()+1;

      return _next === ledger_index;
    });

//console.log("wanted: %s", JSON.stringify(_wanted));
  return _wanted;
};

var set_processed = function (source, ledger_index) {
// console.log("processed% %s > '%s'", JSON.stringify(source), JSON.stringify(processed, undefined, 2));
    var   _processed  = processed[source.currency][source.issuer].insert(ledger_index);

    processed[source.currency][source.issuer] = _processed

    info.processed  = processed;
//console.log("_processed% %s > '%s'", JSON.stringify(source), _processed.to_string());
};

// Persist the state of each processed source.
var replace_processed = function (conn, done) {
  var _rows  = sources.map(function (_source) {
                return [
                  _source.currency,
                  _source.issuer,
                  processed[_source.currency][_source.issuer].to_string(),
                ];
              });

// console.log("REPLACE: ", JSON.stringify(_rows, undefined, 2));

  conn.query("REPLACE Processed (Currency, Account, Done) VALUES ?",
    [_rows],
    function (err, results) {
      if (err)
      {
        console.log("ERR: REPLACE: %s", JSON.stringify({ err: err, results: results}, undefined, 2));
      }
      else {
        info.replaced  = processed;
      }

      done(err);
    });
};

var setup_tables = function () {
  permute([Object.keys(markets), Object.keys(markets)]).forEach(function (_pair) {
      var _ccy1 = _pair[0];
      var _ccy2 = _pair[1];

      if (_ccy1 < _ccy2) {
        // Initialize list of currency pairs.
        pairs.push([_ccy1, _ccy2]);
      }

      if (_ccy1 != _ccy2) {
        // Initialize order book cache.
        books[_ccy1+_ccy2] = {};
      }
  });

  Object.keys(markets).forEach(function (_currency) {
      processed[_currency] = {};

      Object.keys(markets[_currency]).forEach(function (_issuer) {
          // Initialize array of sources.
          sources.push({
              currency: _currency,
              issuer:   _issuer
            });

          // Initialize table of ranges processed for each source.
          processed[_currency][_issuer] = new Range;
        });
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
    conn.query("INSERT Trades (Hash, Market, ApplyIndex, Pays, Gets, LedgerTime, LedgerIndex, Price1, Amount1, Price2, Amount2) VALUES ?",
        [
        records.map(function (r) {
// console.log("* node=%s", JSON.stringify(r, undefined, 2));
          return [ r.Hash, r.Market, r.ApplyIndex, r.Pays, r.Gets, r.LedgerTime, r.LedgerIndex, r.Price1, r.Amount1, r.Price2, r.Amount2 ];
        })],
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

// console.log("l=%s nodes=%s", ledger.ledger_index, t.meta.AffectedNodes.length);
          t.meta.AffectedNodes.forEach(function (n) {
              var base;
              
              if (n.ModifiedNode)
                base  = n.ModifiedNode;
              else if (n.DeletedNode)
                base  = n.DeletedNode;

              if (base
                && base.LedgerEntryType === 'Offer'
                && base.PreviousFields
                && 'TakerPays' in base.PreviousFields
                && 'TakerGets' in base.PreviousFields) {
                var pf          = base.PreviousFields;
                var ff          = base.FinalFields;
// console.log("- l=%s node=%s", ledger.ledger_index, JSON.stringify(base));
                var taker_pays    = Amount.from_json(pf.TakerPays).subtract(Amount.from_json(ff.TakerPays));
                var taker_gets    = Amount.from_json(pf.TakerGets).subtract(Amount.from_json(ff.TakerGets));

                var pays_currency = taker_pays.currency().to_human();
                var pays_issuer   = taker_pays.issuer().to_json();
                var gets_currency = taker_gets.currency().to_human();
                var gets_issuer   = taker_gets.issuer().to_json();

                if (taker_gets.is_positive()
                  && taker_pays.is_positive()
                  && markets[pays_currency]
                  && (markets[pays_currency][pays_issuer] || pays_currency == 'XRP')
                  && markets[gets_currency]
                  && (markets[gets_currency][gets_issuer] || gets_currency == 'XRP'))
                {
// console.log("* l=%s node=%s", ledger.ledger_index, JSON.stringify(base));
//  console.log("l=%s t=%s", ledger.ledger_index, JSON.stringify(t));
                  var record = {
                    TransactionIndex: t.TransactionIndex,
                    Hash:         t.hash,
                    Market:       pays_currency < gets_currency
                                    ? pays_currency+gets_currency
                                    : gets_currency+pays_currency,
                    Pays:         pays_currency,
                    Gets:         gets_currency,
                    LedgerTime:   ledger.close_time,
                    LedgerIndex:  ledger.ledger_index,
                    Price1:       taker_gets.ratio_human(taker_pays).to_human({
                                      precision: 8,
                                      group_sep: false,
                                    }),
                    Amount1:      taker_pays.to_human({
                                      precision: 8,
                                      group_sep: false,
                                    }),
                    Price2:       taker_pays.ratio_human(taker_gets).to_human({
                                      precision: 8,
                                      group_sep: false,
                                    }),
                    Amount2:      taker_gets.to_human({
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

                  // console.log("KEEP: %s %s > %s %s", pays_currency, gets_currency, pays_currency, gets_currency);
                  trades.push(record);

                  // console.log("TRADE: %s for %s", taker_pays.to_human_full(), taker_gets.to_human_full());

                  // console.log("Node: ", JSON.stringify(n, undefined, 2));
                }
              }
            });

          if (trades.length)
          {
            // Trades should be order always better to worse quality.
            // Better quality is a smaller number of pays/gets so the value increases.
            // The natural price will be pays/gets. Regardless of view.
            trades.sort(function (a,b) { return Number(b.Price1)-Number(a.Price1); });  // Normal order: lowest first

            var i = 0;
            
            trades.forEach(function (t) { t.ApplyIndex=i++; });      // Enumerate trades.

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

              if (!('checkpoint_always' in config) || config.checkpoint_always)
              {
                replace_processed(conn, done);
              }
              else
              {
                done();
              }
            }
            else
            {
              done(err);
            }
          });
      })
    .request();
};

// Process what we can based on validated_ledgers
// We have the genesis ledger.
var process_range = function (conn, validated_ledgers, done)
{
  // Do wanted strictly in order.
  // XXX Later to handle missing ledgers, add a list of ledgers to skip in the config.
  
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

// Build the trade database.
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
        // console.log("Connected: %s", JSON.stringify(err, undefined, 2));

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

// _ccy1, _ccy2 : The view point to compute.
var orderbook_add = function (_ccy1, _ccy2, _taker_pays, _taker_gets) {
  // _ccy2 per _ccy1
  var _side = 'bids';

  if (_taker_gets.currency().to_human() === _ccy1) {
    var _gets = _taker_gets;
    var _pays = _taker_pays;

    _taker_gets = _pays;
    _taker_pays = _gets;

    _side       = 'asks';
  }

  var _price        = _taker_gets.ratio_human(_taker_pays).to_human({
                            precision: 8,
                            group_sep: false,
                          });

  // _ccy1
  var _amount       = _taker_pays.to_human({
                            precision: 8,
                            group_sep: false,
                          });
// console.log("orderbook_add: %s%s %s %s %s %s", _ccy1, _ccy2, _price, _amount,_taker_pays.to_human(), _taker_gets.to_human());

  if (Number(_price)) {
// console.log("orderbook_add: %s%s %s %s", _ccy1, _ccy2, JSON.stringify(_taker_gets), _side);
// console.log("orderbook_add: %s%s _price: %s %s %s %s", _ccy1, _ccy2, _side, Number(_price), _price, _amount);
    books[_ccy1+_ccy2][_side].push( [ _price, _amount ] );
  }
};

// Add orders to bids and asks.
//
// books[ BTCUSD ] = books[ ccy1ccy2 ]
//   bids
//     ==            biding USD     buying BTC
//     == offers offer_pays USD offer_gets BTC  <= offer creators point of view
//     == offers taker_gets USD taker_pays BTC  <= someone taking the offer.
//     == waiting for someone to sell BTC
var orderbook_build = function (_ccy1, _ccy2, _offers) {
// console.log("orderbook_build: %s%s %s", _ccy1, _ccy2, JSON.stringify(_offers));
// console.log("orderbook_build: %s%s %s", _ccy1, _ccy2, _offers.length);
  _offers
    .forEach(function (o) {
        // console.log("OFFER: ", JSON.stringify(o, undefined, 2));

        var _taker_pays = Amount.from_json('taker_pays_funded' in o ? o.taker_pays_funded : o.TakerPays);
        var _taker_gets = Amount.from_json('taker_gets_funded' in o ? o.taker_gets_funded : o.TakerGets);

        if (_taker_pays.is_positive() && _taker_gets.is_positive()) {
          orderbook_add(_ccy1, _ccy2, _taker_pays, _taker_gets);
          orderbook_add(_ccy2, _ccy1, _taker_pays, _taker_gets);  // The other view point.
        }
      });
};

// Generate .text.
var orderbook_output = function (_ccy1, _ccy2, _ledger_index) {
// console.log("orderbook_output: %s%s %s", _ccy1, _ccy2, _ledger_index);
// console.log("orderbook_output: bids: %s", books[_ccy1+_ccy2].bids.length);
// console.log("orderbook_output: asks: %s", books[_ccy1+_ccy2].asks.length);
  
  // Sort orders.
  // asks = low to high
  books[_ccy1+_ccy2].asks.sort(function (a, b) { return Number(a[0])-Number(b[0]); });
  // bids = high to low
  books[_ccy1+_ccy2].bids.sort(function (a, b) { return Number(b[0])-Number(a[0]); });

// console.log("orderbook_output: bids: %s asks: %s", books[_ccy1+_ccy2].bids.length, books[_ccy1+_ccy2].asks.length);
// console.log("orderbook_output: asks: %s", JSON.stringify(books[_ccy1+_ccy2].asks, undefined, 2));
  // Build output.
  books[_ccy1+_ccy2].text  = JSON.stringify({
      bids: books[_ccy1+_ccy2].bids,
      asks: books[_ccy1+_ccy2].asks
    }, undefined, 2);

  // Set version.
  books[_ccy1+_ccy2].ledger_index = _ledger_index;

  // Announce to waiters.
// console.log("orderbook_output: emit: %s%s", _ccy1, _ccy2);
  books[_ccy1+_ccy2].processing.emit(_ccy1+_ccy2);

  // No longer processing.
  delete books[_ccy1+_ccy2].processing;
};

// <-- { code: 123, text: foo }
var orderbook_get = function (_ccy1, _ccy2, _done) {
// console.log("orderbook_get: %s%s", _ccy1, _ccy2);
  if (books[_ccy1+_ccy2].ledger_index === info.ledger.ledger_index) {
    // Cached
    // console.log("orderbook_get: cached: %s%s", _ccy1, _ccy2);

    _done({
        code: 200,
        text: books[_ccy1+_ccy2].text
      });
  }
  else if (books[_ccy1+_ccy2].processing) {
    // In process.
  
    // console.log("orderbook_get: listen: %s%s", _ccy1, _ccy2);

    // Listen for the end result.
    books[_ccy1+_ccy2]
      .processing
      .once(_ccy1+_ccy2, function () {
          // console.log("orderbook_get: event: %s%s", _ccy1, _ccy2);
          orderbook_get(_ccy1, _ccy2, _done);
        });
  }
  else {
    // Need to get and build it.
    var _ledger_index = info.ledger.ledger_index;

    var _process      = new EventEmitter();

    books[_ccy1+_ccy2].processing  = _process;
    books[_ccy2+_ccy1].processing  = _process;

    // Listen for end result.
    orderbook_get(_ccy1, _ccy2, _done);

    [ 'bids', 'asks' ].forEach(function (_side) {
        books[_ccy1+_ccy2][_side] = [];
        books[_ccy2+_ccy1][_side] = [];
      });

    // Build source pairs: both sides of books we need to get.
    var _source_pairs  = 
      permute([
        sources.filter(function (_source) {
            return _source.currency === _ccy1;
          }),
        sources.filter(function (_source) {
            return _source.currency === _ccy2;
          })
        ]);

    // Get the other side of each book too.
    var _pairs_both = _source_pairs.concat(_source_pairs.map(function (_pair) {
        return [].concat(_pair).reverse();
      }));

// console.log("sources: ", JSON.stringify(sources, undefined, 2));
// console.log("_source_pairs: ", JSON.stringify(_source_pairs));
    async.each(_pairs_both,
        function (_pair, _callback) {
            remote.request_book_offers(_pair[0], _pair[1])
              .ledger_index(_ledger_index)
              .on('success', function (m) {
// console.log("*** book_offers: %s %s", JSON.stringify(_pair), _ledger_index);
// console.log("*** book_offers: %s %s %s", JSON.stringify(_pair), _ledger_index, JSON.stringify(m.offers));
                  orderbook_build(_pair[0].currency, _pair[1].currency, m.offers);
                  _callback();
                })
              .on('error', function (m) {
                  console.log("ERROR BOOK: ", JSON.stringify(m, undefined, 2));

                  _callback(m);
                })
              .request();
          },
        function (err) {
          if (err) {
// console.log("orderbook_get: err");
            _done({
                code: 500,
                text: "Internal server error."
              });
          }
          else {
// console.log("orderbook_get: all");
            // Have all the offers.

            orderbook_output(_ccy1, _ccy2, _ledger_index);
            orderbook_output(_ccy2, _ccy1, _ledger_index);
          }
        });
  }
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
              + ") ENGINE = " + config.table_type + ";";

          conn.query(sql_create_processed, function (err, results) {
              // console.log("create_processed: %s", JSON.stringify(results, undefined, 2));

              callback(err);
            })
        },
        function (callback) {
            var sql_create_transactions =
                "CREATE TABLE Trades ("
              + "  Market       CHARACTER(6),"                    // Market           (e.g. BTCUSD)
              + "  Pays         CHARACTER(3),"                    // Unit.            (e.g. BTC)
              + "  Gets         CHARACTER(3),"                    // Quoted by price. (e.g. USD)
              + "  ApplyIndex   INTEGER UNSIGNED,"                // Order applied in transaction.
              + "  LedgerTime   INTEGER UNSIGNED,"                // ledger_time
              + "  LedgerIndex  INTEGER UNSIGNED,"                // ledger_index
              + "  Hash         CHARACTER(32),"
              + "  Price1       VARCHAR(32),"                     // CCY2 per CCY1 (e.g. USD per BTC for BTCUSD)
              + "  Amount1      VARCHAR(32),"                     // How many units of CCY1.
              + "  Price2       VARCHAR(32),"                     // CCY1 per CCY2 (e.g. USD per BTC for BTCUSD)
              + "  Amount2      VARCHAR(32),"                     // How many units of CCY2.
              + "  Tid          INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,"
              + ""
              + "  UNIQUE idiom (Hash, ApplyIndex), "
              + "  UNIQUE name (Market, Tid)"
              + ") ENGINE = " + config.table_type + ";";

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

//    req.on('data', function (buffer) {
//        // console.log("DATA: %s", buffer);
//        // input = input + buffer;
//      });

      req.on('end', function () {
          // console.log("END");

          if (httpd_config.cors)
            res.setHeader("Access-Control-Allow-Origin", "*");

          var _parsed = url.parse(req.url, true);
          var _m;

          // console.log("URL: %s", req.url);
          // console.log("HEADERS: %s", JSON.stringify(req.headers, undefined, 2));
          // console.log("INPUT: %s:", JSON.stringify(input));
          // console.log("_parsed: %s", JSON.stringify(_parsed, undefined, 2));

          if (_parsed.pathname === "/") {
            info.processing = self.processing;

            res.statusCode = 200;
            res.end(JSON.stringify(info, undefined, 2));
          }
          else if (_m = _parsed.pathname.match(/^\/(...)\/(...)\/trades.json$/)) {
            var   _ccy1   = _m[1] && _m[1] in markets && _m[1];
            var   _ccy2   = _m[2] && _m[2] in markets && _m[2];
            var   _since  = _parsed.query.since;

            if (!_ccy1 || !_ccy2) {
              res.statusCode = 404;
              res.end(JSON.stringify({
                  message:  'Bad market. Available markets: ' + Object.keys(markets).join(", "),
                  market:   _ccy1+_ccy2
                }, undefined, 2));
            }
            else if (!_since || !_since.match(/^\d+$/)) {
              res.statusCode = 404;
              res.end(JSON.stringify({
                  trades: true,
                  message: 'Missing since.'
                }, undefined, 2));
            }
            else {
              db_perform(function (err, conn, done) {
                  if (err) {
                    done(err);
                  }
                  else {
                    var _market = _ccy1 < _ccy2 ? _ccy1+_ccy2 : _ccy2+_ccy1;

                    conn.query("SELECT LedgerTime, Pays, Price1, Price2, Amount1, Amount2, Tid FROM Trades WHERE Tid > ? AND Market=? ORDER BY Tid ASC LIMIT ?",
                      [Number(_since), _market, config.trade_limit],
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
                                        price:  _ccy1 == r.Pays ? r.Price1 : r.Price2, 
                                        amount: _ccy1 == r.Pays ? r.Amount1 : r.Amount2,
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
                      }, undefined, 2));
                  }
                });
            }
          }
          else if (_m = _parsed.pathname.match(/^\/(...)\/(...)\/orderbook.json$/)) {
            var   _ccy1   = _m[1];
            var   _ccy2   = _m[2];

            if (!(_ccy1 in markets) || !(_ccy2 in markets) || _ccy1 === _ccy2) {
              res.statusCode = 404;
              res.end(JSON.stringify({
                  message:  'Bad market. Available markets: ' + Object.keys(markets).join(", "),
                  market:   _ccy1+_ccy2
                }, undefined, 2));
            }
            else {
              orderbook_get(_ccy1, _ccy2, function (m) {
                  res.statusCode = m.code;
                  res.end(m.text);
                });
            }
          }
          else
          {
            res.statusCode = 404;
            res.end(JSON.stringify({
                message: 'File not found.',
                parsed: _parsed,
              }, undefined, 2));
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
                process_validated(m.validated_ledgers); // Build trade database.
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
