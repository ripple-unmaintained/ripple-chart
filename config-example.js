//
// ripple-chart: config-example.js
//

// mysql -u 'newcoin' -pfoobar rbc
exports.mysql_config = {
  user:     'rbc',
  host:     'localhost',
  password: 'foobar',
  database: 'rbc',
};

// Where to serve http requests.
exports.httpd_config = {
  ip:   "0.0.0.0",
  port: 9005,
};

// This will run much, much faster if your run against a local rippled.
exports.rippled_config = {
  trusted:        true,         // true, if you trust the rippled.
  websocket_ip:   "127.0.0.1",
  websocket_port: 7005,
  websocket_ssl:  false,        // false, if your connection is local.
};

// Specify the notable accounts to cross.
//
// TODO: This table could be improved by changing the account value to be the
// ledger of the creation of the account. This would allow process for an
// account to begin more appropriately and would remove need to specify the
// genesis ledger.
exports.markets = {
  AUD: {
    rBcYpuDT1aXNo4jnqczWJTytKGdBGufsre: 'weex',
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
  },
  BTC: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
    rpvfJ4mR6QQAeogpXEKnuyGBx8mYCSnYZi: 'weex',
  },
  CAD: {
    r47RkFi1Ew3LvCNKT6ufw3ZCyj5AJiLHi9: 'weex',
  },
  CHF: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
  },
  EUR: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
  },
  GBP: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
  },
  JPY: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
  },
  USD: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
    r9vbV3EHvXWjSkeQ6CAcYVPGeq7TuiXY2X: 'weex',
  },
  XRP: {
     rrrrrrrrrrrrrrrrrrrrrhoLvTp:       'ripples',
  }
};

// rchart.js will not begin building the database unless, the server it
// connects to can provide the genesis ledger below.
//
// There is probably no reason to change these:
exports.config = {
//genesis_ledger:     32570,      // Effective genesis ledger.
  genesis_ledger:     152370,     // First notable trade here.
  trade_limit:        1000,       // Maximum number of trades to return at once.
  table_type:         'MyISAM',   // ISAM, MyISAM (no INNODB)
  checkpoint_always:  true,       // Checkpoint during initial build.
};

// vim:sw=2:sts=2:ts=8:et
