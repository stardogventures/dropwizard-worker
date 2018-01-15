*** dropwizard-worker ***

by Ian White, Stardog Ventures (@eonwhite)

This is a simple way to run asynchronous, decentralized jobs from a
Dropwizard service.

This is particularly useful for crons scheduled with Amazon CloudWatch,
as described in this great post by Dan McKinley (@mcfunley):

   https://blog.skyliner.io/a-simple-pattern-for-jobs-and-crons-on-aws-2f965e43932f
   
But this worker also should be suitable for busy event-processing queues as well.

The basic pattern is:

  - Create any number of jobs you want to run. They need to implement
  `Consumer<Map<String,Object>>`, where the `Map` represents parameters
  to your method.

```java
class MyCron implements Consumer<<Map<String,Object>> {
    public void consume(Map<String,Object> params) {
        // do something interesting here
    }
}
```
  
  - Configure a WorkerService with your configuration, list of methods,
  and the specific worker you are intending to use for processing messages
  (normally `SqsWorker`)
  
```java
env.lifecycle().manage(new WorkerService(
        "worker",
        WorkerConfig.builder().maxThreads(20).build(),
        ImmutableList.of(
                WorkMethod.of("MyCron", new MyCron())),
                WorkMethod.of("AnotherCron", new AnotherCron()))
        ),
        new SqsWorker(sqsClient, "queue-name", env.metrics()),
        env.metrics());
```  
  
  - Send to the queue (from anywhere in your distributed system) messages in the format:
  
```
  {"method":<method>}
  or
  {"method":<method>,"params":<params>}
  or
  {"method":<method>,"params":<params>,"at":<millis>}
```

Where `method` is the name of the method, `params` is an object containing
your parameters, and `at` is a millisecond timestamp of the time the message was
queued.

Features:
  - Scales up for busy queues, launching threads and polling as quickly
  as possible, but automatically scaling down for infrequently used queues
  to save money and resources.
  - Builtin metrics for monitoring the state of your queue.
  - Includes a Dropwizard Task to run methods manually.