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

public class QueueInfo {
  private String exchangeName;
  private String inMsgQueue;
  private String inRawMsgQueue;
  private String outMsgQueue;
  private String outRawMsgQueue;

  public QueueInfo() {
  }

  public QueueInfo(String exchangeName, String inMsgQueue, String inRawMsgQueue, String outMsgQueue, String outRawMsgQueue) {
    this.exchangeName = exchangeName;
    this.inMsgQueue = inMsgQueue;
    this.inRawMsgQueue = inRawMsgQueue;
    this.outMsgQueue = outMsgQueue;
    this.outRawMsgQueue = outRawMsgQueue;
  }

  public String getExchangeName() {
    return exchangeName;
  }

  public void setExchangeName(String exchangeName) {
    this.exchangeName = exchangeName;
  }

  public String getInMsgQueue() {
    return inMsgQueue;
  }

  public void setInMsgQueue(String inMsgQueue) {
    this.inMsgQueue = inMsgQueue;
  }

  public String getInRawMsgQueue() {
    return inRawMsgQueue;
  }

  public void setInRawMsgQueue(String inRawMsgQueue) {
    this.inRawMsgQueue = inRawMsgQueue;
  }

  public String getOutMsgQueue() {
    return outMsgQueue;
  }

  public void setOutMsgQueue(String outMsgQueue) {
    this.outMsgQueue = outMsgQueue;
  }

  public String getOutRawMsgQueue() {
    return outRawMsgQueue;
  }

  public void setOutRawMsgQueue(String outRawMsgQueue) {
    this.outRawMsgQueue = outRawMsgQueue;
  }
}
