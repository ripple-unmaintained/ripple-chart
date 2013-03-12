
var Range = function () {
  this.value	= [];
};

Range.from_string = function (str) {
  var range	= new Range();

  range.value	= str.split(",").map(function (str_interval) {
      var interval  = str_interval.split("-");

      return 1 === interval.length ? Number(interval) : interval;
    });

  console.log("RANGE: ", JSON.stringify(range.value));

  return range;
};

//var intersect_raw = function (a, b) {
//  var result  = new Range;
//
//  if (a.length && b.length)
//  {
//    var a_head  = Number === typeof a[0] ? a[0] : a[1];
//    var b_head  = Number === typeof b[0] ? b[0] : b[1];
//    var a_tail  = Number === typeof a[0] ? a[0] : a[a.length-1];
//    var b_tail  = Number === typeof b[0] ? b[0] : b[b.length-1];
//
//    intersect_bare(a_head, a_tail, b_head, b_tail);
//
//    if (a_head < b_head) {
//      interset_bare(a_
//    }
//  }
//
//  return result;
//};

Range.prototype.empty = function () {
  return !this.value.length;
};

Range.prototype.has_member = function (n) {
  return !this.value.every(function (r) {
      // Return true if n is not r.

      return 'number' === typeof r
        ? n !== r
        : (n < r[0] || n > r[1]);
    });
};

exports.Range = Range;

// vim:sw=2:sts=2:ts=8:et
