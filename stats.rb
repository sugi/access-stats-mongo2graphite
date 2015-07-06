#!/usr/bin/ruby

require 'graphite-api'
require 'mongo'
require 'pp'
require 'active_support/time'
require 'getoptlong'

Mongo::Logger.logger.level = Logger::INFO

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


def usage
  puts <<-EOS
#{$0} [options]

Options:
 -h, --help               Show help
 -v, --verbose            Verbose output (multiple for more)
 -q, --quiet              Quiet output (multiple for more)
 -b, --back-min=MINUTS    Gather stat from MINUTS ago (default: 1 hour)
 -f, --from=TIME_STRING   Set gathering start point by time
 -d, --mongodb-url=URL    MongoDB URL (default: mongodb://localhost:27017/test)
 -c, --mongodb-col=COL    MongoDB collection name (default: access)
 -g, --graphire-server=S  Graphite server (default: localhost:2003)
 -p, --graphite-prefix=P  Graphite metric prefix (default: '')
 -m, --map=JS_FILE        JavaScript file for map function (default: map.js)
 -r, --reduce=JS_FILE     JavaScript file for reduce (default: reduce.js)
 -F, --finalize=JS_FILE   JavaScript file for finalize (default: finalize.js)
EOS
end

opts = GetoptLong.new(
  [ '--help', '-h', GetoptLong::NO_ARGUMENT ],
  [ '--back-min', '-b', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--from', '-f', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--verbose', '-v', GetoptLong::NO_ARGUMENT ],
  [ '--quiet', '-q', GetoptLong::NO_ARGUMENT ],
  [ '--graphite-server', '-g', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--graphite-prefix', '-p', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--mongodb-url', '-d', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--mongodb-col', '-c', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--map', '-m', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--reduce', '-r', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--finalize', '-F', GetoptLong::REQUIRED_ARGUMENT ],
)

time_from = Time.now - 1.hour
map_js = File.dirname(__FILE__)+'/map.js'
reduce_js = File.dirname(__FILE__)+'/reduce.js'
finalize_js = File.dirname(__FILE__)+'/finalize.js'
graphite_server = 'localhost:2003'
graphite_prefix = []
mongodb_url = 'mongodb://localhost:27017/test'
mongodb_col = 'access'

opts.each do |opt, arg|
    case opt
    when '--help'
      usage
      exit
    when '--verbose'
      Mongo::Logger.logger.level -= 1
    when '--quiet'
      Mongo::Logger.logger.level += 1
    when '--back-min'
      arg.to_i == 0 and raise "Invalid value for back min: #{arg}"
      time_from = Time.now - arg.to_i.minutes
    when '--from'
      time_from = Time.parse(arg)
    when '--map'
      map_js = arg
    when '--reduce'
      reduce_js = arg
    when '--finalize'
      finalize_js = arg
    when '--graphite-server'
      graphite_server = arg
    when '--graphite-prefix'
      graphite_prefix = arg.split('.')
    when '--mongodb-url'
      mongodb_url = arg
    when '--mongodb-col'
      mongodb_col = arg
  end
end

map = File.read(map_js)
reduce = File.read(reduce_js)
finalize = File.read(finalize_js)

gh = GraphiteAPI.new(graphite: graphite_server)

mdb = Mongo::Client.new(mongodb_url)
mr_opts = {finalize: finalize, query: {}, sort: {time: 1}}

if time_from
  mr_opts[:query][:time] ||= {}
  mr_opts[:query][:time]['$gte'] = time_from
end

Mongo::Logger.logger.info "Gathering stats from #{time_from}..."

count = 0
mdb[mongodb_col].find.map_reduce(map, reduce, mr_opts).each do |stat|
  count += 1
  time = stat.delete '_id'
  gh.metrics to_dotted_flat_hash(stat['value'], graphite_prefix), Time.at(time.to_i)
end

Mongo::Logger.logger.info "Done, #{count} data points has been inserted."
