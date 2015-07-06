function(key, v) {
  for (var k in v) {
    if (typeof(v[k]) != "object") continue;
    for (var kk in v[k]) {
      if (kk == '_total' && v[k]._count) {
        v[k]._avg = v[k]._total / v[k]._count;
      }
    }
  }
  return v;
}
