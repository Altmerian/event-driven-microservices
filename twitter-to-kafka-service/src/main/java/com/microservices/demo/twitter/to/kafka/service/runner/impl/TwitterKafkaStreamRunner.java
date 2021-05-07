package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import java.util.Arrays;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
  private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
  private final TwitterKafkaStatusListener twitterKafkaStatusListener;
  private TwitterStream twitterStream;

  @Override
  public void start() throws TwitterException {
    twitterStream = new TwitterStreamFactory().getInstance().addListener(twitterKafkaStatusListener);
    addFilter();
  }

  @PreDestroy
  public void shutdown() {
    if (twitterStream != null) {
      LOG.info("closing twitter stream...");
      twitterStream.shutdown();
    }
  }

  private void addFilter() {
    String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(String[]::new);
    FilterQuery filterQuery = new FilterQuery(keywords);
    twitterStream.filter(filterQuery);
    LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
  }
}
