package org.apache.flume.sink.gbase;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.conf.Configurable;

public interface HttpSinkHandler extends Configurable {
    public long handle(HttpServletRequest request, HttpServletResponse response);
}
