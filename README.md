# rchart.js - ripple-chart

## Usage

Run as a web server:

    rchart.js serve

Initialize dbs:

     rchart.js reset

## Theory

This constructs a database suitable for servering to bitcoincharts.com as per:
>[http://bitcoincharts.com/about/exchanges/](http://bitcoincharts.com/about/exchanges/)

Each time the database is created, the results may vary. Therefore, if the
database is lost, the new database will be inconsistent with the previous
database. All previous subscribers should subscribe from scratch to have a
consistent database.

To create, the cleanest database, the program will not being importation until
the rippled server has acquired the effective genesis ledger.

Serves pages at:

* [http://a.b.c.d/](http://a.b.c.d/)
* [http://a.b.c.d/ABC/XYZ/trades.json?since=1234](http://a.b.c.d/ABC/XYZ/trades.json?since=1234)
* [http://a.b.c.d/ABC/XYZ/orderbook.json](http://a.b.c.d/ABC/XYZ/orderbook.json)

For Bitcoincharts, they would query only the `https://ripple.com/chart/BTC/` portion of the pages.

## Installation

* `npm install async mysql@2.0.0-alpha7 ripple-lib`
* `cp -p config-examples.js config.js`
* Edit `config.js` as needed.
* Set up MySQL.

### MySql Configuration

    DROP DATABASE IF EXISTS rbc;

    CREATE DATABASE rbc;

    GRANT ALL PRIVILEGES ON rbc.* to 'rbc'@'localhost' IDENTIFIED BY 'password';

## Operation

The program can be interrupted at anytime.

Add curriences and markets at anytime.  Then, restart.

This warning is expected:

    (node) warning: possible EventEmitter memory leak detected.

## Live Server

Currently Opencoin Inc runs this server at:

* [https://ripple.com/chart/](https://ripple.com/chart/)

Pages for bitcoincharts.com:

* [https://ripple.com/chart/BTC/USD/trades.json?since=0](https://ripple.com/chart/BTC/USD/trades.json?since=0)
* [https://ripple.com/chart/BTC/USD/orderbook.json](https://ripple.com/chart/BTC/USD/orderbook.json)

