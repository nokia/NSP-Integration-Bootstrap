# NSP Kafka consumer example in Python
---
##### How to run the Kafka python consumer example

- *Python* and *pip* tools must be installed.
- It's recommended to use [Virtualenv](https://virtualenv.pypa.io/en/stable/) to have a separate virtual environment that is an isolated working copy of Python without worrying about affecting other projects

```sh
$ virtualenv env
$ source env/bin/activate
$ pip install -r requirements.txt
$ python kafka_test_consumer.py -host ${nsp_host_ip} -topic ${nsp_subscription_topic_id}
```


Be sure to swap out:
- `${nsp_host_ip}` with your NSP host IP address
- `${nsp_subscription_topic_id}` with the actual topic id that you get from sending a Kafka subscription request to NSP

