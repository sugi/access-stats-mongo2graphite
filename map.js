function() {
  var timeKey = function (d) {
    d.setSeconds(0);
    d.setMilliseconds(0);
    return d.getTime()/1000;
  }
  if (!this.remote_country) return;
  var r = {count: 1, code: {}, method: {}, size: {_val: this.size}};
  r.method[this.method] = 1;
  r.code[this.code.toString()] = 1;
  if (this.process_time)
    r['process_time'] = {_val: this.process_time}
  if (this.remote_country) {
    r['country'] = {}
    r.country[this.remote_country] = 1;
  }
  if (this.webfront) {
    r['webfront'] = {}
    r.webfront[this.webfront] = 1;
  }
  emit(timeKey(this.time), r);
}
