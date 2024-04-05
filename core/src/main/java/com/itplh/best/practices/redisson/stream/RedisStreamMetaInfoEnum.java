package com.itplh.best.practices.redisson.stream;

import lombok.Getter;

@Getter
public enum RedisStreamMetaInfoEnum {

    STREAM_MESSAGE_STRING("stream-string-queue", "default_group", "default_consumer", "default_message_event"),
    STREAM_MESSAGE_MAP("stream-map-queue", "default_group", "default_consumer", "default_message_event"),
    STREAM_MESSAGE_DATE("stream-date-queue", "default_group", "default_consumer", "default_message_event"),
    ;

    private String stream;
    private String group;
    private String consumer;
    private String eventKey;

    RedisStreamMetaInfoEnum(String stream, String group, String consumer, String eventKey) {
        this.stream = stream;
        this.group = group;
        this.consumer = consumer;
        this.eventKey = eventKey;
    }

}
