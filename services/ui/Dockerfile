FROM provectuslabs/kafka-ui:latest

USER root

ARG STOREPASS

RUN apk update && \
    apk upgrade && \
    apk add wget kafkacat && \
    mkdir --parents /usr/local/share/ca-certificates/Yandex/ && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
         --output-document /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt && \
    chmod 0655 /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt

RUN mkdir /truststore && \
    keytool -import \
            -file /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
            -storepass ${STOREPASS} \
            -alias "kafka-ui-cert" \
            -keystore /truststore/truststore.jks \
            -noprompt
