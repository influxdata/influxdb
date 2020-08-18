# ActiveMQ Input Plugin

This plugin gather queues, topics & subscribers metrics using ActiveMQ Console API.

### Configuration:

```toml
# Description
[[inputs.activemq]]
  ## ActiveMQ WebConsole URL
  url = "http://127.0.0.1:8161"

  ## Required ActiveMQ Endpoint
  ##   deprecated in 1.11; use the url option
  # server = "192.168.50.10"
  # port = 8161

  ## Credentials for basic HTTP authentication
  # username = "admin"
  # password = "admin"

  ## Required ActiveMQ webadmin root path
  # webadmin = "admin"

  ## Maximum time to receive response.
  # response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

Every effort was made to preserve the names based on the XML response from the ActiveMQ Console API.

- activemq_queues
  - tags:
    - name
    - source
    - port
  - fields:
    - size
    - consumer_count
    - enqueue_count
    - dequeue_count
+ activemq_topics
  - tags:
    - name
    - source
    - port
  - fields:
    - size
    - consumer_count
    - enqueue_count
    - dequeue_count
- activemq_subscribers
  - tags:
    - client_id
    - subscription_name
    - connection_id
    - destination_name
    - selector
    - active
    - source
    - port
  - fields:
    - pending_queue_size
    - dispatched_queue_size
    - dispatched_counter
    - enqueue_counter
    - dequeue_counter

### Example Output

```
activemq_queues,name=sandra,host=88284b2fe51b,source=localhost,port=8161 consumer_count=0i,enqueue_count=0i,dequeue_count=0i,size=0i 1492610703000000000
activemq_queues,name=Test,host=88284b2fe51b,source=localhost,port=8161 dequeue_count=0i,size=0i,consumer_count=0i,enqueue_count=0i 1492610703000000000
activemq_topics,name=ActiveMQ.Advisory.MasterBroker\ ,host=88284b2fe51b,source=localhost,port=8161 size=0i,consumer_count=0i,enqueue_count=1i,dequeue_count=0i 1492610703000000000
activemq_topics,host=88284b2fe51b,name=AAA\,source=localhost,port=8161  size=0i,consumer_count=1i,enqueue_count=0i,dequeue_count=0i 1492610703000000000
activemq_topics,name=ActiveMQ.Advisory.Topic\,source=localhost,port=8161 ,host=88284b2fe51b enqueue_count=1i,dequeue_count=0i,size=0i,consumer_count=0i 1492610703000000000
activemq_topics,name=ActiveMQ.Advisory.Queue\,source=localhost,port=8161 ,host=88284b2fe51b size=0i,consumer_count=0i,enqueue_count=2i,dequeue_count=0i 1492610703000000000
activemq_topics,name=AAAA\ ,host=88284b2fe51b,source=localhost,port=8161 consumer_count=0i,enqueue_count=0i,dequeue_count=0i,size=0i 1492610703000000000
activemq_subscribers,connection_id=NOTSET,destination_name=AAA,,source=localhost,port=8161,selector=AA,active=no,host=88284b2fe51b,client_id=AAA,subscription_name=AAA pending_queue_size=0i,dispatched_queue_size=0i,dispatched_counter=0i,enqueue_counter=0i,dequeue_counter=0i 1492610703000000000
```
