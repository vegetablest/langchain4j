package dev.langchain4j.store.memory.chat.redis;

import com.redis.testcontainers.RedisContainer;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.UserMessage;
import lombok.SneakyThrows;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.redis.testcontainers.RedisStackContainer.DEFAULT_IMAGE_NAME;
import static com.redis.testcontainers.RedisStackContainer.DEFAULT_TAG;

class RedisChatMemoryStoreTest implements WithAssertions {

    static RedisContainer redis = new RedisContainer(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG))
            .withEnv("REDIS_ARGS", "--requirepass redis-stack");

    @BeforeAll
    static void setUp() {
        redis.start();
    }

    @AfterAll
    static void tearDown() {
        redis.stop();
    }

    @Test
    public void testBuilder() {
        RedisChatMemoryStore store = RedisChatMemoryStore.builder()
                .host(redis.getHost())
                .port(redis.getFirstMappedPort())
                .password("redis-stack")
                .keyPrefix("chat:memories:")
                .ttl(1000)
                .build();
        assertThat(store.getTtl()).isEqualTo(1000);
        assertThat(store.getKeyPrefix()).isEqualTo("chat:memories:");
    }

    @Test
    public void testStore() {
        RedisChatMemoryStore redisChatMemoryStore = RedisChatMemoryStore.builder()
                .host(redis.getHost())
                .port(redis.getFirstMappedPort())
                .password("redis-stack")
                .build();
        assertThat(redisChatMemoryStore.getMessages("foo")).isEmpty();

        redisChatMemoryStore.updateMessages("foo", Arrays.asList(new UserMessage("abc def"), new AiMessage("ghi jkl")));

        assertThat(redisChatMemoryStore.getMessages("foo"))
                .containsExactly(new UserMessage("abc def"), new AiMessage("ghi jkl"));

        redisChatMemoryStore.deleteMessages("foo");

        assertThat(redisChatMemoryStore.getMessages("foo")).isEmpty();
    }

    @Test
    @SneakyThrows
    public void testTtl() {
        RedisChatMemoryStore redisChatMemoryStore = RedisChatMemoryStore.builder()
                .host(redis.getHost())
                .port(redis.getFirstMappedPort())
                .password("redis-stack")
                .ttl(5)
                .build();

        redisChatMemoryStore.updateMessages("bar", Collections.singletonList(new UserMessage("hello world")));

        assertThat(redisChatMemoryStore.getMessages("bar")).containsExactly(new UserMessage("hello world"));

        TimeUnit.SECONDS.sleep(6);
        assertThat(redisChatMemoryStore.getMessages("bar")).isEmpty();
    }

    @Test
    public void testPassword() {

        RedisChatMemoryStore storeAndPassword = RedisChatMemoryStore.builder()
                .host(redis.getHost())
                .port(redis.getFirstMappedPort())
                .password("redis-stack")
                .build();
        Exception exception = catchException(() ->
                storeAndPassword.updateMessages("bar", Collections.singletonList(new UserMessage("hello world"))));
        assertThat(exception).isNull();

        RedisChatMemoryStore storeWithoutPassword = RedisChatMemoryStore.builder()
                .host(redis.getHost())
                .port(redis.getFirstMappedPort())
                .build();
        Exception authException = catchException(() ->
                storeWithoutPassword.updateMessages("bar", Collections.singletonList(new UserMessage("hello world"))));
        assertThat(authException).isNotNull();
        assertThat(authException).isInstanceOf(JedisDataException.class);
    }
}