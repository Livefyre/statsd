/*jshint node:true, laxcomma:true */

var util = require('util'),
  AWS = require('aws-sdk'),
  zlib = require('zlib'),
  graphite = require('./graphite.js');


function gzipBody(data, callback) {
  zlib.gzip(new Buffer(data.Body, 'utf-8'), function (_, result) {
    data.Body = result;
    data.ContentEncoding = "gzip";
    data.ContentLength = result.length;
    callback(data);
  });
}


function S3Backend(startupTime, config, emitter) {
  var self = this;
  this.lastFlush = startupTime;
  this.lastException = startupTime;
  this.config = config.s3;
  this.bufferedStats = [];
  this.bufferedTimestamps = [];
  this.config.sparse = this.config.sparse === true;
  this.config.precision = this.config.precision || 0;
  this.config.maxUploadAttempts = this.config.maxUploadAttempts || 3;
  this.config.failedUploadDelay = this.config.failedUploadDelay || 15 * 1000;
  this.config.types = this.config.types || {
    counter: 'counter',
    gauge: 'gauge',
    set: 'set',
    timer: 'timer'
  };

  //graphite.init(startupTime, config, emitter);

  AWS.config.update({ accessKeyId: this.config.accessKeyId, secretAccessKey: this.config.secretAccessKey });

  // attach
  emitter.on('flush', function (ts, stats, force) {
    var statsString = self.constructPayload(ts, stats, function (ts, payload) {
      self.flush(ts, payload, force);
    });
  });

  emitter.on('status', function (callback) {
    self.status(callback);
  });

}


S3Backend.prototype.constructPayload = function (ts, metrics, writer) {
  // ts,ns,metric,type,value
  var startTime = Date.now(),
    ns = this.config.namespace,
    sparse = this.config.sparse,
    payload = [],
    numStats = 0,
    key = null,
    timer_data_key,
    v = null,
    counter_type = this.config.types.counter,
    gauge_type = this.config.types.gauge,
    timer_type = this.config.types.timer,
    set_type = this.config.types.set,
    precision = this.config.precision;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    var value = counters[key];
    if (sparse && value === 0) {
      continue;
    }
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate
    payload.push([ts, ns, key, counter_type,
      value.toFixed(precision), valuePerSecond.toFixed(precision)].join(","));
  }

  for (key in timer_data) {
    for (timer_data_key in timer_data[key]) {
      if (typeof (timer_data[key][timer_data_key]) === 'number') {
        payload.push([ts, ns, key, timer_type,
          timer_data[key][timer_data_key].toFixed(precision), ''].join(","));
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          payload.push([ts, ns, key + '.' + timer_data_key + '.' + timer_data_sub_key, timer_type,
            timer_data[key][timer_data_key][timer_data_sub_key].toFixed(precision), ''].join(","));
        }
      }
    }
  }

  for (key in gauges) {
    payload.push([ts, ns, key, gauge_type, gauges[key].toFixed(precision), ''].join(","));
  }

  for (key in sets) {
    if (sparse && value === 0) {
      continue;
    }
    payload.push([ts, ns, key, set_type, sets[key].values().length, ''].join(","));
  }
  writer(ts, payload.join("\n"));
};

S3Backend.prototype.flush = function (timestamp, statString, callback) {
  var self = this;
  this.bufferedTimestamps.push(timestamp);
  this.bufferedStats.push(statString);
  if (!callback && this.bufferedStats.length < this.config.numBufferedIntervals) {
    console.log('Buffering stats at', new Date(timestamp * 1000).toString());
    return;
  }
  console.log('Flushing stats at', new Date(timestamp * 1000).toString());

  var data = {
    Key: this.config.keyName(this.bufferedTimestamps),
    ContentType: "text/plain; charset=utf-8",
    Body: this.bufferedStats.join("\n")
  };

  this.bufferedTimestamps = [];
  this.bufferedStats = [];

  gzipBody(data, function (result) {
    if (self.config.debug) {
      console.log(statString);
      console.log(result);
    }
    if (self.config.noUpload) {
      if (callback) {
        callback();
      }
      return;
    }
    self.upload(result, 0, callback);
  });
};

S3Backend.prototype.upload = function (data, tries, callback) {
  var self = this;

  if (tries >= self.config.maxUploadAttempts) {
    console.error("Max attempts exceeded, abandoning the payload");
    if (callback) {
      callback(self.lastException);
    }
    return;
  }

  new AWS.S3({params: {Bucket: this.config.bucket}}).putObject(data, function (err, res) {
    if (err) {
      self.lastException = err;
      console.error("Failed to upload; retrying: " + err);
      setTimeout(function () {
        self.upload(data, tries + 1, callback);
      }, self.config.failedUploadDelay);
      return;
    }
    console.log('Successfully uploaded: ' + data['Key'] + ": " + res);
    if (callback) {
      callback(null, data, res);
    }
  });

}

S3Backend.prototype.status = function (write) {
  ['lastFlush', 'lastException'].forEach(function (key) {
    write(null, 'console', key, this[key]);
  }, this);
};


exports.init = function (startupTime, config, events) {
  var instance = new S3Backend(startupTime, config, events);
  return true;
};

exports.gzip = gzipBody;


