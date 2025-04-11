FROM confluentinc/cp-kafka:7.9.0

USER root
COPY topic-test.sh /usr/local/bin/kafka-test.sh
RUN chmod +x /usr/local/bin/kafka-test.sh
USER appuser

ENTRYPOINT ["/usr/local/bin/kafka-test.sh"]
