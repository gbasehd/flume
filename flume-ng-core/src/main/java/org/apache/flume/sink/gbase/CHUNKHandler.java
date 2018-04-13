package org.apache.flume.sink.gbase;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.Context;

public class CHUNKHandler implements HttpSinkHandler {

    @Override
    public long handle(HttpServletRequest request, HttpServletResponse response) {
        return 0;
    }

    @Override
    public void configure(Context context) {
    }

}
