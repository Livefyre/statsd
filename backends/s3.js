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
  config.writer = config.s3.writer || function (ts, stats, force) {
    self.flush(ts, stats, force);
  };

  graphite.init(startupTime, config, emitter);

  AWS.config.update({ accessKeyId: this.config.accessKeyId, secretAccessKey: this.config.secretAccessKey });
}

S3Backend.prototype.flush = function (timestamp, statString, force) {
  this.bufferedTimestamps.push(timestamp);
  this.bufferedStats.push(statString);
  if (!force && this.bufferedStats.length < this.config.numBufferedIntervals) {
    console.log('Buffering stats at', new Date(timestamp * 1000).toString());
    return;
  }
  console.log('Flushing stats at', new Date(timestamp * 1000).toString());

  var s3bucket = new AWS.S3({params: {Bucket: this.config.bucket}});

  var data = {
    Key: this.config.keyName(this.bufferedTimestamps),
    ContentType: "text/plain; charset=utf-8",
    Body: this.bufferedStats.join("\n")
  };

  this.bufferedTimestamps = [];
  this.bufferedStats = [];

  gzipBody(data, function (result) {
    console.log(result);
    if (force) {
      force();
    }
    /*
    s3bucket.putObject(result).done(function (resp) {
      console.log('Successfully uploaded: ' + data['Key'] + resp);
    });
    */
  });
};


exports.init = function (startupTime, config, events) {
  var instance = new S3Backend(startupTime, config, events);
  return true;
};

exports.gzip = gzipBody;


