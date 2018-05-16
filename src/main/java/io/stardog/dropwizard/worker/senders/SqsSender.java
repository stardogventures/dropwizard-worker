package io.stardog.dropwizard.worker.senders;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.interfaces.Sender;
import io.stardog.dropwizard.worker.util.WorkerDefaults;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.UncheckedIOException;

public class SqsSender implements Sender {
    private final AmazonSQS sqs;
    private final String defaultQueueName;
    private final String defaultMessageGroupId;
    private final ObjectMapper mapper;

    @Inject
    public SqsSender(AmazonSQS sqs, @Named("sqsQueueName") String defaultQueueName, @Named("sqsMessageGroupId") String defaultMessageGroupId, ObjectMapper mapper) {
        this.sqs = sqs;
        this.defaultQueueName = defaultQueueName;
        this.defaultMessageGroupId = defaultMessageGroupId;
        this.mapper = mapper;
    }

    public SqsSender(AmazonSQS sqs, String defaultQueueName, @Nullable String defaultMessageGroupId) {
        this(sqs, defaultQueueName, defaultMessageGroupId, WorkerDefaults.MAPPER);
    }

    @Override
    public void send(WorkMessage message) {
        send(message, defaultQueueName, defaultMessageGroupId);
    }

    public void send(WorkMessage message, String queueName, @Nullable String messageGroupId) {
        try {
            String body = mapper.writeValueAsString(message);
            SendMessageRequest request = new SendMessageRequest()
                    .withQueueUrl(sqs.getQueueUrl(queueName).getQueueUrl())
                    .withMessageBody(body);
            if (messageGroupId != null) {
                    request.withMessageGroupId(messageGroupId);
            }
            sqs.sendMessage(request);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
