package io.stardog.dropwizard.worker.workers;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.WorkMethods;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.interfaces.ManagedWorker;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.util.WorkerDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Simple worker that reads messages off a particular SQS queue and processes them.
 *
 * Records the following meters:
 *   SqsWorker.[queuename].received
 *      number of messages received
 *   SqsWorker.[queuename].processed
 *      number of messages successfully processed
 *   SqsWorker.[queuename].error
 *      number of messages erroring out
 *   SqsWorker.[queuename].error.[method]
 *      number of errors per method type
 *   SqsWorker.[queuename].error.parse
 *      number of errors specifically related to malformed messages unable to parse
 *
 *  And the following timers:
 *    SqsWorker.[queuename].delay
 *      amount of time messages are spending waiting in the queue
 *    SqsWorker.[queuename].timer.[method]
 *      amount of time messages of each type are taking to process
 */
@Singleton
public class SqsWorker implements ManagedWorker, Managed {
    private final AmazonSQS sqs;
    private final String sqsName;
    private String sqsUrl;
    private final WorkMethods methods;
    private final MetricRegistry metrics;
    private final ObjectMapper mapper;
    private final static Logger LOGGER = LoggerFactory.getLogger(SqsWorker.class);

    @Inject
    public SqsWorker(WorkMethods methods, AmazonSQS sqs, @Named("sqsName") String sqsName, MetricRegistry metrics, ObjectMapper mapper) {
        this.methods = methods;
        this.sqs = sqs;
        this.sqsName = sqsName;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    public SqsWorker(WorkMethods methods, AmazonSQS sqs, String sqsName, MetricRegistry metrics) {
        this(methods, sqs, sqsName, metrics, WorkerDefaults.MAPPER);
    }

    public SqsWorker(WorkMethods methods, AmazonSQS sqs, String sqsName) {
        this(methods, sqs, sqsName, new MetricRegistry(), WorkerDefaults.MAPPER);
    }

    @Override
    public void start() throws Exception {
        this.sqsUrl = sqs.getQueueUrl(sqsName).getQueueUrl();
    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public boolean processMessages() {
        if (sqsUrl == null) {
            throw new IllegalStateException("Called processMessages on SqsWorker without calling start() lifecycle method");
        }
        ReceiveMessageRequest request = new ReceiveMessageRequest(sqsUrl);
        List<Message> messages = sqs.receiveMessage(request).getMessages();

        if (messages.size() == 0) {
            return false;
        }

        metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "received")).mark(messages.size());
        LOGGER.debug("Received " + messages.size() + " messages");

        for (Message message : messages) {
            LOGGER.debug("Processing message: " + message);
            WorkMessage workMessage;
            try {
                workMessage = parseMessage(message);
            } catch (Exception e) {
                LOGGER.warn("Exception parsing: " + message.getBody());
                metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "error")).mark();
                metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "error", "parse")).mark();
                continue;
            }

            try {
                boolean processed = processMessage(workMessage);
                if (processed) {
                    sqs.deleteMessage(new DeleteMessageRequest(sqsUrl, message.getReceiptHandle()));
                    metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "processed")).mark();
                } else {
                    metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "skipped")).mark();
                }

            } catch (Exception e) {
                LOGGER.warn("Exception processing: " + message.getBody(), e);
                metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "error")).mark();
                metrics.meter(MetricRegistry.name(SqsWorker.class, sqsName, "error", workMessage.getMethod())).mark();
            }
        }

        LOGGER.debug("Completed processing " + messages.size() + " messages");

        return true;
    }

    protected WorkMessage parseMessage(Message message) throws IOException {
        return mapper.readValue(message.getBody(), WorkMessage.class);
    }

    protected boolean processMessage(WorkMessage message) {
        long startTime = System.currentTimeMillis();

        if (message.getQueueAt().isPresent()) {
            long queueTime = message.getQueueAt().get().toEpochMilli();
            long delayTime = Math.max(0, queueTime - startTime);
            metrics.timer(MetricRegistry.name(SqsWorker.class, sqsName, "delay"))
                    .update(delayTime, TimeUnit.MILLISECONDS);
        }

        WorkMethod workMethod = methods.getMethod(message.getMethod());
        boolean result = workMethod.getFunction().apply(message.getParams());
        long endTime = System.currentTimeMillis();

        metrics.timer(MetricRegistry.name(SqsWorker.class, sqsName, "timer", message.getMethod()))
                .update(endTime - startTime, TimeUnit.MILLISECONDS);

        return result;
    }
}
