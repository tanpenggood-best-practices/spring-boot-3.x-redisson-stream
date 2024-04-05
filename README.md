## Spring Boot 3.x Redisson Stream

I use the best practices of Redis stream in spring boot 3.x and redisson.

### Feature

- [x] Verify FullName(stream + group + consumer_name) **not blank** and **duplicate**, when register Listener
- [x] `com.itplh.best.practices.redisson.stream.EnhanceStreamListener` add the following extensions:
    1. getMetaInfo()
    1. getStream()
    1. getGroup()
    1. getConsumerName()
    1. getFullName()
    1. getFullGroup()
- [x] Abstract class `com.itplh.best.practices.redisson.stream.AbstractAutoRetryStreamListener`
    1. Automatically enable the failed retry mechanism, when the stream is single consume group
        1. default maximum retry count: 10
        1. customize the override of the 'maxRetries' method
    1. Support successful callback ` onSuccess`
    1. Support failure callback (reaching the max retries) `onFailure`
    1. Support finally callback `onFinally`

### Development Environment

- Java 21
- Redis 5.0.10
- Spring Boot 3.2.4
- redisson-spring-boot-starter 3.27.2
- Lombok 1.18.30

### Sample

1. Run `com.itplh.best.practices.redisson.Application`
2. push data to stream
    ```bash
    curl --location --request GET 'http://localhost:8080/redisson/stream/push-data'
    ```

### Docs

[Redis Stream - Best Practices](http://showdoc.itplh.com/web/#/4?page_id=382)

[Should XDEL be executed after XACK ?](http://showdoc.itplh.com/web/#/4?page_id=403)