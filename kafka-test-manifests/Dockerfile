FROM confluentinc/cp-kafka:7.9.0

USER root

# Create directory for scripts
RUN mkdir -p /usr/local/bin/kafka-test-modules

# Copy main scripts
COPY scripts/kafka-test-modular.sh /usr/local/bin/kafka-test.sh
COPY scripts/topic-test-modular.sh /usr/local/bin/topic-test.sh

# Copy module scripts
COPY scripts/kafka-test-modules/*.sh /usr/local/bin/kafka-test-modules/

# Set execute permissions
RUN chmod +x /usr/local/bin/kafka-test.sh \
    && chmod +x /usr/local/bin/topic-test.sh \
    && chmod +x /usr/local/bin/kafka-test-modules/*.sh

USER appuser

# Default entry point is the main test script
ENTRYPOINT ["/usr/local/bin/kafka-test.sh"]