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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

public class HDHTTestOperator extends AbstractSinglePortHDHTWriter<Double>
{
  private long windowId;
  private transient boolean called = false;

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;

    byte[] result = null;

    if (!called) {
      called = true;
      try {
        result = this.get(0, new Slice(GPOUtils.serializeInt(0)));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      if (result != null) {
        long storedWindowId = GPOUtils.deserializeLong(result);

        if (storedWindowId < windowId) {
          LOG.info("The stored Window ID is less than the window ID {} {}", storedWindowId, windowId);
        }
      }
    }
  }

  @Override
  public void endWindow()
  {
    try {
      this.put(0, new Slice(GPOUtils.serializeInt(0)), GPOUtils.serializeLong(windowId));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected HDHTCodec<Double> getCodec()
  {
    return new MyCodec();
  }

  public class MyCodec implements HDHTCodec<Double>, Serializable
  {
    private static final long serialVersionUID = 201510020616L;

    @Override
    public byte[] getKeyBytes(Double event)
    {
      return null;
    }

    @Override
    public byte[] getValueBytes(Double event)
    {
      return null;
    }

    @Override
    public Double fromKeyValue(Slice slice, byte[] bytes)
    {
      return null;
    }

    @Override
    public Object fromByteArray(Slice fragment)
    {
      return null;
    }

    @Override
    public Slice toByteArray(Double o)
    {
      return null;
    }

    @Override
    public int getPartition(Double o)
    {
      return 0;
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HDHTTestOperator.class);
}
