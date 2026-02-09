from __future__ import annotations

import os
from typing import Any


def _split_csv(value: str) -> list[str]:
  return [v.strip() for v in value.split(',') if v.strip()]


# Defaults keep compatibility with the original Yandex Managed Kafka setup,
# but everything can be overridden via env vars for local docker-compose.
kafka_bootstrap_servers = _split_csv(
  os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'rc1a-cpi67jpnj0k2fih0.mdb.yandexcloud.net:9091',
  )
)

kafka_security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
kafka_sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-512')
kafka_ssl_cafile = os.getenv(
  'KAFKA_SSL_CAFILE',
  '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt',
)

kafka_input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'inputs')
kafka_output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'predictions')


def kafka_client_kwargs(username: str | None = None, password: str | None = None) -> dict[str, Any]:
  """Build kwargs for kafka-python(-ng) clients.

  Supports both local PLAINTEXT (docker-compose) and managed SASL_SSL.
  """
  kwargs: dict[str, Any] = {
    'bootstrap_servers': kafka_bootstrap_servers,
  }

  security_protocol = kafka_security_protocol.upper()
  kwargs['security_protocol'] = security_protocol

  if security_protocol == 'PLAINTEXT':
    return kwargs

  # SASL/SSL case
  kwargs['sasl_mechanism'] = kafka_sasl_mechanism
  kwargs['ssl_cafile'] = kafka_ssl_cafile
  if username is not None:
    kwargs['sasl_plain_username'] = username
  if password is not None:
    kwargs['sasl_plain_password'] = password
  return kwargs

