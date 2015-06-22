#!/usr/bin/ruby

require 'graphite-api'
require 'mongo'
require 'pp'
require 'active_support/time'

Mongo::Logger.logger.level = Logger::INFO

map = <<-EOS
  function() {
    var timeKey = function (d) {
      d.setSeconds(0);
      d.setMilliseconds(0);
      return d.getTime()/1000;
    }
    if (!this.remote_country) return;
    var r = {count: 1, country: {}, code: {}, webfront: {}, method: {},
             process_time: {_val: this.process_time},
             size: {_val: this.size}};
    r.country[this.remote_country] = 1;
    r.code[this.code.toString()] = 1;
    r.webfront[this.webfront] = 1;
    r.method[this.method] = 1;
    emit(timeKey(this.time), r);
  }
EOS
reduce = <<-EOS
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
EOS
avg = <<-EOS
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
EOS

def to_dotted_flat_hash(h, parents = [])
  ret = {}
  h.each do |k, v|
    if v.kind_of? Hash
      ret.update to_dotted_flat_hash(v, parents + [k])
    else
      ret[(parents + [k]).map{|i| i.tr('/.', '__')}.join('.')] = v
    end
  end
  return ret
end

stamp_file = "stats.stamp"
time_gap = 1.hour

time_from = nil
begin
  time_from = Time.at(File.read(stamp_file).to_i)
rescue
  # ignore
end
t = Time.now - time_gap
time_to = Time.new(t.year, t.month, t.day, t.hour, t.min)

if time_from == time_to
  Mongo::Logger.logger.debug "Noting to do"
  exit
end

gh = GraphiteAPI.new(graphite: 'localhost:2003')

mdb = Mongo::Client.new('mongodb://localhost:27017/mozshot')
mr_opts = {finalize: avg, query: {time: {'$lt' => time_to}}, sort: {time: 1}}

if time_from
  mr_opts[:query][:time]['$gte'] = time_from
end

Mongo::Logger.logger.debug "Gathering stats (#{time_from} - #{time_to})..."

mdb['access'].find.map_reduce(map, reduce, mr_opts).each do |stat|
  time = stat.delete '_id'
  gh.metrics to_dotted_flat_hash(stat['value'], ['mozshot']), Time.at(time.to_i)
end

File.write stamp_file, time_to.to_i
