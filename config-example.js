//
// ripple-chart: config-example.js
//
// mysql -u 'newcoin' -pfoobar rbc
exports.mysql_config = {
  host:     'localhost',
  user:     'newcoin',
  password: 'foobar',
  database: 'rbc',
};

exports.config = {
//  genesis_ledger:     32570,      // Effective genesis ledger.
  genesis_ledger:     152370,     // First notable trade here.
  trade_limit:        1000,       // Maximum number of trades to return at once.
  table_type:         'MyISAM',   // ISAM, MyISAM (no INNODB)
  checkpoint_always:  true,
};

exports.httpd_config = {
  ip:   "0.0.0.0",
  port: 9005,
};

exports.rippled_config = {
  trusted:        true,
  websocket_ip:   "127.0.0.1",
  websocket_port: 7005,
  websocket_ssl:  false,
};

exports.markets = {
  AUD: {
    rBcYpuDT1aXNo4jnqczWJTytKGdBGufsre: 'weex',
  },
  BTC: {
    rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B:  'bitstamp',
    rpvfJ4mR6QQAeogpXEKnuyGBx8mYCSnYZi: 'weex',
  },
  CAD: {
    r47RkFi1Ew3LvCNKT6ufw3ZCyj5AJiLHi9: 'weex',
  },
  EUR: {
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

// vim:sw=2:sts=2:ts=8:et
