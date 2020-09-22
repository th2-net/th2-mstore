/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.messagestore;

import com.exactpro.th2.RabbitMqSubscriber;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConnectivityListener {

  public enum Source {
    IN_MSG,
    IN_RAW,
    OUT_MSG,
    OUT_RAW
  }

  private PublishSubject<ConnectivityMessage> messages;
  private RabbitMqSubscriber inMsgSubscriber;
  private RabbitMqSubscriber inRawSubscriber;
  private RabbitMqSubscriber outMsgSubscriber;
  private RabbitMqSubscriber outRawSubscriber;

  public ConnectivityListener(QueueInfo queueInfo) {
    messages = PublishSubject.create();
    inMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.IN_MSG),
      null,
      queueInfo.getInMsgQueue());
    inRawSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.IN_RAW),
      null,
      queueInfo.getInRawMsgQueue());
    outMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.OUT_MSG),
      null,
      queueInfo.getOutMsgQueue());
    outRawSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.OUT_RAW),
      null,
      queueInfo.getOutRawMsgQueue());
  }

  private DeliverCallback handler(Source sourceQueue) {
    return (String consumerTag, Delivery delivery) ->
      messages.onNext(new ConnectivityMessage(consumerTag, delivery.getBody(), sourceQueue));
  }

  public Flowable<ConnectivityMessage> getFlowable() {
    return messages.toFlowable(BackpressureStrategy.BUFFER);
  }

  public void start() throws IOException, TimeoutException {
    inMsgSubscriber.startListening();
    inRawSubscriber.startListening();
    outMsgSubscriber.startListening();
    outRawSubscriber.startListening();
  }

  public void stop() throws IOException {
    inMsgSubscriber.close();
    inRawSubscriber.close();
    outMsgSubscriber.close();
    outRawSubscriber.close();
    messages.onComplete();
  }
}
