package org.apache.flume.sink.gbase;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.conf.Configurable;

public interface PassiveHttpSinkHandler extends Configurable {
  public void setSink(Sink sink);
  public long handle(HttpServletRequest request, HttpServletResponse response)
      throws EventDeliveryException;
}
