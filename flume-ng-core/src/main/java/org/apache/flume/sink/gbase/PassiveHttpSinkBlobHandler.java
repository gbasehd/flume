package org.apache.flume.sink.gbase;

import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PassiveHttpSinkBlobHandler implements PassiveHttpSinkHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PassiveHttpSinkBlobHandler.class);
  private Sink sink;
  private CounterGroup counterGroup = new CounterGroup();
  private int batchSize = GBase8aSinkConstants.DFLT_BATCH_SIZE;
  private String contentType = GBase8aSinkConstants.DFLT_CONTENT_TYPE;
  private String characterEncoding = GBase8aSinkConstants.DFLT_CHARACTER_ENCODING;
  
  @Override
  public long handle(HttpServletRequest request, HttpServletResponse response)
      throws EventDeliveryException {
    checkRequest(request);
    
    Channel channel = sink.getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;

    boolean responsePrepared = false;
    long eventSize = 0;
    try {
      transaction.begin();
      for (eventSize = 0; eventSize < batchSize; eventSize++) {
        event = channel.take();
        if (event == null) {
          break;
        }

        if (!responsePrepared) {
          setupResponse(response);
          responsePrepared = true;
        }
        
        // write event to response
        OutputStream stream = response.getOutputStream();
        stream.write(event.getBody());
        
        // flush buffer to send data immediately
        response.flushBuffer();
      }

      if (eventSize > 0) {
        transaction.commit();
        counterGroup.addAndGet("events.success", (long) Math.min(batchSize, eventSize));
        counterGroup.incrementAndGet("transaction.success");
      } else {
        transaction.rollback();
        counterGroup.incrementAndGet("transaction.failed");
        LOG.warn("No event to deliver.");
        response.sendError(HttpServletResponse.SC_NO_CONTENT, "No event to deliver.");
      }
    } catch (Exception ex) {
      transaction.rollback();
      counterGroup.incrementAndGet("transaction.failed");
      LOG.error("Failed to deliver event. Exception follows.", ex);
      throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    } finally {
      transaction.close();
    }

    return eventSize;
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger(GBase8aSinkConstants.BATCH_SIZE,
        GBase8aSinkConstants.DFLT_BATCH_SIZE);
    characterEncoding = context.getString(GBase8aSinkConstants.CHARACTER_ENCODING,
        GBase8aSinkConstants.DFLT_CHARACTER_ENCODING);
    contentType = context.getString(GBase8aSinkConstants.CONTENT_TYPE,
        GBase8aSinkConstants.DFLT_CONTENT_TYPE);

    counterGroup.setName(sink.getName());
  }

  @Override
  public void setSink(Sink sink) {
    this.sink = sink;
  }

  private void checkRequest(HttpServletRequest request) {
    // throw HTTPBadRequestException if an invalid request received
  }
  
  private void setupResponse(HttpServletResponse response) {
    response.setContentType(contentType);
    response.setCharacterEncoding(characterEncoding);
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
