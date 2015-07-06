function(key, values) {
  var ret = {};
  values.forEach(function(v) {
    for (var k in v) {
      if (typeof(v[k]) == "object") {
        if (typeof(ret[k]) == "undefined") ret[k] = {};
        for (var kk in v[k]) {
          if (kk == '_val') {
            if (typeof(ret[k]._min) == "undefined" ||
                ret[k]._min > v[k]._val) {
              ret[k]._min = v[k]._val;
            }
            if (typeof(ret[k]._max) == "undefined" ||
                ret[k]._max < v[k]._val) {
              ret[k]._max = v[k]._val;
            }
            if (!ret[k]._total) ret[k]._total = 0;
            if (!ret[k]._count) ret[k]._count = 0;
            ret[k]._total += v[k]._val;
            ret[k]._count += 1;
          } else {
            if (!ret[k][kk]) ret[k][kk] = 0;
            ret[k][kk] += v[k][kk];
          }
        }
      } else {
        if (!ret[k]) ret[k] = 0;
        ret[k] += v[k];
      }
    }
  });
  return ret;
}
