# dropwizard-worker

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
  - Build a `WorkerMethods` object that contains your list of `WorkMethods` available. Each
WorkMethod gets a unique name for the method and a consumer. The names are important, since
you'll use those names to call the method remotely.

If you have some really simple methods, you may want to consider inlining them as lambdas.
  
```java
WorkerMethods methods = WorkerMethods.of(ImmutableList.of(
        WorkMethod.of("cron", new MyCron()),
        WorkMethod.of("ping", params -> System.out.println("pong"))
));
```  
  - Configure a WorkerManager with your configuration,
  and the worker you are using for processing messages
  (normally `SqsWorker`)
  
```java
env.lifecycle().manage(new WorkerManager(
        "worker",
        WorkerConfig.builder().maxThreads(20).build(),
        new SqsWorker(sqsClient, "queue-name", env.metrics()),
        env.metrics());
```  
  
  - Send to the queue (from anywhere in your distributed system, such as for example CloudWatch)
  messages in the format:
  
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

  - You can also send messages with a `SqsSender` like so:
  
```java
SqsSender sender = new SqsSender(sqsClient, "queue-name")
```  

Features:
  - Scales up for busy queues, launching threads and polling as quickly
  as possible, but automatically scaling down for infrequently used queues
  to save money and resources.
  - Builtin metrics for monitoring the state of your queue.
  - Includes a Dropwizard Task to run methods manually.
  
#### Timezone-aware crons

While it's generally better to schedule crons in UTC time, there are situations
where you want a cron to run at a time in a local timezone (e.g. "9 am Eastern time"),
adjusting for Daylight Savings time.

CloudWatch however does not support timezone-aware cron scheduling.

So the solution is:

  - Wrap your cron in a `TimeZoneCron` like so:
```java
     TimeZoneCron.of(new MyCron(), ZoneId.of("America/New_York"))
```
  - TimeZoneCron will check for a `dst` property in the params. If `dst`
  is set to `true`, the cron will only run when in Daylight Savings Time,
  and if `dst` is `false`, the cron will only run when not in Daylight
  Savings Time.
  - Schedule *two* otherwise identical CloudWatch SQS events at the two
  potential times (e.g. 9am Eastern time could be either 13:00 UTC or
  14:00 UTC), one with `"dst":true` and one with `"dst":false`
  - TimeZoneCron will run your cron only once, depending on whether it
  is currently DST or not.
  - The above could get screwy if you schedule your cron in the wee hours
  of the morning during the DST changeover period, so don't do that.
  
#### Redis PubSub
  
dropwizard-worker can send and receive messages in the same format using Redis PubSub
via Jedis.

This can be useful instead of SQS in situations where you require
multiple-subscriber behavior -- for example to have your appservers subscribe
to a cache invalidation mechanism.

Because the Jedis client handles its own polling, there is no need to use the `WorkerManager`
with Redis. You can instantiate `RedisWorker` to consume pubsub, and `RedisSender` to send
messages.

See the `RedisExample` example for how this works.