package dev.langchain4j.store.memory.chat.redis;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ChatMessageDeserializer;
import dev.langchain4j.data.message.ChatMessageSerializer;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;
import lombok.Getter;
import redis.clients.jedis.JedisPooled;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;

/**
 * Implementation of {@link ChatMemoryStore} that stores state of {@link dev.langchain4j.memory.ChatMemory} (chat messages) to redis.
 * <p>
 * Compared with {@link dev.langchain4j.store.memory.chat.InMemoryChatMemoryStore} and {@link dev.langchain4j.store.memory.chat.FileChatMemoryStore}, it can also satisfy distributed scenarios.
 */
@Getter
public class RedisChatMemoryStore implements ChatMemoryStore {

    private final JedisPooled client;
    private final Integer ttl;
    private final String keyPrefix;

    /**
     * Constructs a new RedisChatMemoryStore instance.
     *
     * @param host      the Redis server host
     * @param port      the Redis server port
     * @param user      the Redis Stack username (optional)
     * @param password  the Redis Stack password (optional)
     * @param keyPrefix the name prefix of keys in Redis (optional)
     * @param ttl       the expiration time of keys in Redis (optional)
     */
    private RedisChatMemoryStore(
            String host, Integer port, String user, String password, String keyPrefix, Integer ttl) {
        ensureNotBlank(host, "host");
        ensureNotNull(port, "port");
        ensureNotNull(keyPrefix, "keyPrefix");
        if (Objects.nonNull(ttl)) {
            ensureGreaterThanZero(ttl, "ttl");
        }
        this.client = Objects.isNull(password) ? new JedisPooled(host, port) :
                new JedisPooled(host, port, user, password);
        this.ttl = ttl;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public List<ChatMessage> getMessages(Object memoryId) {
        List<String> messages = client.lrange(keyPrefix + memoryId.toString(), 0, -1);
        return messages.stream().map(ChatMessageDeserializer::messageFromJson).collect(Collectors.toList());
    }

    @Override
    public void updateMessages(Object memoryId, List<ChatMessage> messages) {
        deleteMessages(memoryId);
        List<String> jsonMessages =
                messages.stream().map(ChatMessageSerializer::messageToJson).collect(Collectors.toList());
        jsonMessages.forEach(jsonMessage -> client.rpush(keyPrefix + memoryId.toString(), jsonMessage));
        if (Objects.nonNull(ttl)) {
            client.expire(keyPrefix + memoryId.toString(), ttl);
        }
    }

    @Override
    public void deleteMessages(Object memoryId) {
        client.del(keyPrefix + memoryId);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String host;
        private Integer port;
        private String user;
        private String password;
        private String keyPrefix = "message_store:";
        private Integer ttl;

        /**
         * @param host Redis server host.
         * @return builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * @param port Redis server port.
         * @return builder
         */
        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        /**
         * @param user Redis Stack username (optional).
         * @return builder
         */
        public Builder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * @param password Redis Stack password (optional).
         * @return builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * @param keyPrefix The name prefix of key in redis (optional).
         * @return builder
         */
        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        /**
         * @param ttl Expiration time of key in redis.
         * @return builder
         */
        public Builder ttl(Integer ttl) {
            this.ttl = ttl;
            return this;
        }

        public RedisChatMemoryStore build() {
            return new RedisChatMemoryStore(host, port, user, password, keyPrefix, ttl);
        }
    }
}
