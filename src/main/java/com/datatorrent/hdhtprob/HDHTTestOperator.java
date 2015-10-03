/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.hdhtprob;

import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.LoggerFactory;

public class HDHTTestOperator extends AbstractSinglePortHDHTWriter<Double>
{
  private long windowId;
  private transient boolean called = false;
  private transient Random rand = new Random();
  private int numBuckets = 6;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;

    if (!called) {
      called = true;

      for(int bucketCounter = 0; bucketCounter < numBuckets; bucketCounter++) {
        checkBucket(bucketCounter);
      }
    }
  }

  @Override
  public void processEvent(Double val)
  {
    int key = rand.nextInt(Integer.MAX_VALUE - 1) + 1;

    try {
      this.get(rand.nextInt(numBuckets), new Slice(GPOUtils.serializeInt(key)));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    try {
      this.put(rand.nextInt(numBuckets), new Slice(GPOUtils.serializeInt(key)), GPOUtils.serializeDouble(val));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void checkBucket(long bucket)
  {
    byte[] result = null;

    try {
      result = this.get(bucket, new Slice(GPOUtils.serializeInt(0)));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    if (result != null) {
      long storedWindowId = GPOUtils.deserializeLong(result);

      if (storedWindowId > windowId) {
        LOG.info("The stored Window ID is less than the window ID {} {}", storedWindowId, windowId);
      }
    }
  }

  @Override
  public void endWindow()
  {
    for (int bucketCounter = 0; bucketCounter < numBuckets; bucketCounter++) {
      putBucket(bucketCounter);
    }

    super.endWindow();
  }

  private void putBucket(int bucketCounter)
  {
    try {
      this.put(bucketCounter, new Slice(GPOUtils.serializeInt(0)), GPOUtils.serializeLong(windowId));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected HDHTCodec<Double> getCodec()
  {
    return new MyCodec();
  }

  public static class MyCodec implements HDHTCodec<Double>, Serializable
  {
    private static final long serialVersionUID = 201510020616L;

    @Override
    public byte[] getKeyBytes(Double event)
    {
      return GPOUtils.serializeDouble(event);
    }

    @Override
    public byte[] getValueBytes(Double event)
    {
      return GPOUtils.serializeDouble(event);
    }

    @Override
    public Double fromKeyValue(Slice slice, byte[] bytes)
    {
      return null;
    }

    @Override
    public Object fromByteArray(Slice fragment)
    {
      return (Double) GPOUtils.deserializeDouble(fragment.buffer, new MutableInt(0));
    }

    @Override
    public Slice toByteArray(Double o)
    {
      return new Slice(GPOUtils.serializeDouble(o));
    }

    @Override
    public int getPartition(Double o)
    {
      return 0;
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HDHTTestOperator.class);
}
