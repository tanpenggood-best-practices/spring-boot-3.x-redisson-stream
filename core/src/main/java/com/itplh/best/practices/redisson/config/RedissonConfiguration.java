package com.itplh.best.practices.redisson.config;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.CompositeCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.redisson.spring.starter.RedissonAutoConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedissonConfiguration {

    private final ObjectMapper objectMapper;

    @Bean
    public RedissonAutoConfigurationCustomizer redissonCustomizer() {
        return config -> {
            ObjectMapper om = objectMapper.copy();
            om.setVisibility(PropertyAccessor.ALL, Visibility.ANY);
            // 指定序列化输入的类型，类必须是非final修饰的。序列化时将对象全类名一起保存下来
            om.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);
            TypedJsonJacksonCodec jsonCodec = new TypedJsonJacksonCodec(Object.class, om);
            // 组合序列化 key 使用 String 内容使用通用 json 格式
            CompositeCodec codec = new CompositeCodec(StringCodec.INSTANCE, jsonCodec, jsonCodec);
            config.setCodec(codec);
            log.info("初始化 redisson client 配置");
        };
    }

}
