package io.stardog.dropwizard.worker.workers;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.data.WorkerConfig;
import io.stardog.dropwizard.worker.WorkerService;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SqsWorkerTest {
    private AmazonSQSClient sqsClient;
    private MetricRegistry metrics;
    private SqsWorker worker;
    private WorkerService service;
    private AtomicBoolean didWork = new AtomicBoolean(false);

    @Before
    public void setUp() throws Exception {
        sqsClient = mock(AmazonSQSClient.class);
        metrics = new MetricRegistry();
        worker = new SqsWorker(sqsClient, "test-sqs", metrics);

        WorkerConfig config = WorkerConfig.builder().incIntervalMillis(10).build();
        List<WorkMethod> workMethods = ImmutableList.of(
                WorkMethod.of("testMethod", (params) -> { didWork.set(true); } )
        );

        service = new WorkerService(
                "worker-service", config, workMethods, worker, new MetricRegistry());

        didWork.set(false);
    }

    @Test
    public void processMessages() throws Exception {
        when(sqsClient.getQueueUrl("test-sqs"))
                .thenReturn(new GetQueueUrlResult().withQueueUrl("https://example.com/url"));
        worker.start();

        ReceiveMessageResult emptyResult = new ReceiveMessageResult();
        emptyResult.setMessages(ImmutableSet.of());

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(emptyResult);

        assertFalse(worker.processMessages(service));
        assertFalse(didWork.get());

        ReceiveMessageResult result = new ReceiveMessageResult();
        Message message = new Message().withBody("{\"method\":\"testMethod\"}");
        result.setMessages(ImmutableSet.of(message));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(result);

        assertTrue(worker.processMessages(service));
        assertTrue(didWork.get());

        verify(sqsClient, times(1)).deleteMessage(any());
    }
}