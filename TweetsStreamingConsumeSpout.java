


package com.sinfonier.spouts;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
public class TweetsStreamingConsumeSpout extends BaseSinfonierSpout {
private SpoutOutputCollector collector;
private LinkedBlockingQueue<Status> queue;
private TwitterStream twitterStream;
@Override
public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
this.collector = collector;
this.twitterStream = new TwitterStreamFactory().getInstance();
this.queue = new LinkedBlockingQueue<Status>();
final StatusListener listener = new StatusListener() {
@Override
public void onStatus(Status status) {
queue.offer(status);
}
@Override
public void onDeletionNotice(StatusDeletionNotice sdn) {
}
@Override
public void onTrackLimitationNotice(int i) {
}
@Override
public void onScrubGeo(long l, long l1) {
}
@Override
public void onException(Exception e) {
}
@Override
public void onStallWarning(StallWarning warning) {
}
};
twitterStream.addListener(listener);
}
@Override
public void nextTuple() {
final Status status = queue.poll();
if (status == null) {
Utils.sleep(50);
} else {
collector.emit(new Values(status));
}
}
@Override
public void activate() {
twitterStream.sample();
};
@Override
public void deactivate() {
twitterStream.cleanUp();
};
@Override
public void close() {
twitterStream.shutdown();
}
@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {
declarer.declare(new Fields("status"));
}
}
