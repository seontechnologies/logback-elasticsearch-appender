package com.internetitem.logback.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import com.internetitem.logback.elasticsearch.config.ElasticsearchProperties;
import com.internetitem.logback.elasticsearch.config.HttpRequestHeaders;
import com.internetitem.logback.elasticsearch.config.Settings;
import com.internetitem.logback.elasticsearch.util.ErrorReporter;
import com.internetitem.logback.elasticsearch.writer.ElasticsearchWriter;
import com.internetitem.logback.elasticsearch.writer.SafeWriter;
import com.sun.tools.javac.util.List;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;


@RunWith(MockitoJUnitRunner.class)
public class ClassicElasticsearchPublisherTest {
    private static final int MAX_REQUEST_SIZE = 5242880;

    @Mock
    private Context context;
    @Mock
    private Settings settings;
    @Mock
    ErrorReporter errorReporter;
    @Mock
    private ElasticsearchProperties properties;
    @Mock
    private HttpRequestHeaders headers;

    private ClassicElasticsearchPublisher publisher;

    private Field fldWorking;
    private Field fldOutputAggregator;

    @Before
    public void setUp() throws IOException, NoSuchFieldException {
        when(settings.getMaxQueueSize()).thenReturn(MAX_REQUEST_SIZE);
        when(settings.getSleepTime()).thenReturn(150);
        when(settings.getIndex()).thenReturn("logs");
        when(settings.getType()).thenReturn("someLogs");
        when(settings.getUrl()).thenReturn(new URL("https://my.sample.elastic"));
        publisher = new ClassicElasticsearchPublisher(context, errorReporter, settings, properties, headers);
        fldWorking = AbstractElasticsearchPublisher.class.getDeclaredField("working");
        fldWorking.setAccessible(true);
        fldOutputAggregator = AbstractElasticsearchPublisher.class.getDeclaredField("outputAggregator");
        fldOutputAggregator.setAccessible(true);

        Answer<Void> loggerAnswer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                String message = invocation.getArgumentAt(0, String.class);
                System.out.println(message);
                return null;
            }
        };
        doAnswer(loggerAnswer).when(errorReporter).logWarning(anyString());
        doAnswer(loggerAnswer).when(errorReporter).logInfo(anyString());
    }

    @Test
    public void retry_does_not_get_stuck_if_es_fails() throws InterruptedException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, IOException {
        // given

        final boolean[] shouldStop = new boolean[]{false};
        final boolean[] failed = new boolean[]{false};
        final int[] maxIndex = new int[]{0, 0};
        Thread loggingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                int limitPerTime = 300;
                long timeInterval = 10;
                int i = 1;
                while (!shouldStop[0]) {
                    try {
                        ILoggingEvent event = mock(ILoggingEvent.class);
                        when(event.getLevel()).thenReturn(Level.INFO);
                        String currentMsg = "_____" + i + "++++" +
                          "OuIpju4ZK7RRHLJ5VgihDFXf5yLvq8NVErzXZ6HDW2Cq1xadj";
                        when(event.getFormattedMessage()).thenReturn(
                          currentMsg
                        );
                        publisher.addEvent(event);
                        maxIndex[0] = i;
                        i += 1;
                        if (i % limitPerTime == 0) {
                            long sleepTime = start + timeInterval - System.currentTimeMillis();
                            if (sleepTime > 0) {
                                Thread.sleep(sleepTime);
                            }
                            start = System.currentTimeMillis();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "Logging thread");
        loggingThread.setPriority(Thread.MIN_PRIORITY);
        loggingThread.start();
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        ElasticsearchOutputAggregator aggregator = (ElasticsearchOutputAggregator)fldOutputAggregator.get(publisher);
        Field f = ElasticsearchOutputAggregator.class.getDeclaredField("writers");
        f.setAccessible(true);
        ArrayList<SafeWriter> writers = (ArrayList<SafeWriter>) f.get(aggregator);
        final ElasticsearchWriter writer = spy((ElasticsearchWriter)writers.get(0));
        f.set(aggregator, List.of(
          writer
        ));
        mockWriterSendData(writer, maxIndex, shouldStop, failed);
        // when

        waitForPublisherThreadToStartWorking();

        loggingThread.join();

        waitForPublisherThreadToFinish();
        shouldStop[0] = true;
        if (failed[0]) {
            Assert.fail("A critical failure occured");
        }
        assertEquals(maxIndex[0], maxIndex[1]);
    }

    private void mockWriterSendData(final ElasticsearchWriter writer, final int[] maxIndex, final boolean[] shouldStop, final boolean[] failed) throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, IOException {

        Field fldBuffer = ElasticsearchWriter.class.getDeclaredField("sendBuffer");
        fldBuffer.setAccessible(true);
        Field fldBackupBuffer = ElasticsearchWriter.class.getDeclaredField("backupBuffer");
        fldBackupBuffer.setAccessible(true);

        final Field fldBufferExceeded = ElasticsearchWriter.class.getDeclaredField("bufferExceeded");
        fldBufferExceeded.setAccessible(true);

        final Method updateBuffersOnSuccess = ElasticsearchWriter.class.getDeclaredMethod("updateBuffersOnSuccess");
        updateBuffersOnSuccess.setAccessible(true);

        final StringBuilder sendBuffer = (StringBuilder)fldBuffer.get(writer);

        final Long startTime = System.currentTimeMillis();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws IllegalAccessException, InvocationTargetException {
                String buf = sendBuffer.substring(sendBuffer.indexOf("\n") + 1, sendBuffer.indexOf("\n", sendBuffer.indexOf("\n") + 1));
                String bufEnd = sendBuffer.substring(sendBuffer.lastIndexOf("\n", sendBuffer.lastIndexOf("\n") - 1));
                System.out.println(sendBuffer.length());
                System.out.println(buf + "\n..." + bufEnd);
                if (System.currentTimeMillis() - startTime > 10000) {
                    shouldStop[0] = true;
                }
                if (sendBuffer.length() > MAX_REQUEST_SIZE) {
                    shouldStop[0] = true;
                    failed[0] = true;
                    throw new RuntimeException("Request too large");
                } else if (sendBuffer.charAt(sendBuffer.length() - 1) != '\n') {
                    shouldStop[0] = true;
                    failed[0] = true;
                    throw new RuntimeException("Request must end with a \n");
                } else {
                    maxIndex[1] = Integer.parseInt(bufEnd.substring(bufEnd.indexOf("____") + 5, bufEnd.indexOf("++++")));
                    // System.out.println(sendBuffer.toString().substring(0, 200));
                    updateBuffersOnSuccess.invoke(writer);
                }
                return null;
            }
        }).when(writer).sendData();
    }

    private void waitForPublisherThreadToStartWorking() throws InterruptedException, IllegalAccessException {
        do {
            boolean working = (boolean) fldWorking.get(publisher);
            Thread.sleep(100);
            if (working) {
                break;
            }
        } while (true);
    }

    private void waitForPublisherThreadToFinish() throws InterruptedException, IllegalAccessException {
        int c = 0;
        do {
            boolean working = (boolean) fldWorking.get(publisher);
            Thread.sleep(100);
            if (!working) {
                break;
            }
            c += 1;
            if (c > 100) {
                Assert.fail("Log publisher thread is not stopping properly");
                break;
            }
        } while (true);
    }
}
