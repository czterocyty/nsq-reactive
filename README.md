# NSQ Java reactive client

Heavily under development.

The NSQ client part was taken from
 [https://github.com/life360/JavaNSQClient.git](https://github.com/life360/JavaNSQClient.git).

## Building prerequisites

* Gradle 4.4
* docker-compose available

## Testing

```
./gradlew clean check
```

There are still TCK tests failing: 
[Reactive Streams TCK](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.2/tck/README.md)
