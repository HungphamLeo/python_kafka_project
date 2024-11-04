import sys
import json
import time
import logging

from utils import (
    GracefulShutdown,
    log_ini,
    save_pid,
    get_hostname,
    log_exception,
    timestamp_now,
    delivery_report,
    get_script_name,
    get_system_config,
    validate_cli_args,
    log_event_received,
    set_producer_consumer,
)


SCRIPT = get_script_name(__file__)
HOSTNAME = get_hostname()

log_ini(SCRIPT)
kafka_config_file, sys_config_file = validate_cli_args(SCRIPT)
SYS_CONFIG = get_system_config(sys_config_file)


# Kafka topics and configurations
PRODUCE_TOPIC_BAKE = SYS_CONFIG['kafka-topics']['pizza_baked']
PRODUCE_TOPIC_STATUS = SYS_CONFIG['kafka-topics']['pizza_status']
CONSUME_TOPICS = [
    SYS_CONFIG['kafka-topics']['pizza_assembled'],
]
_,PRODUCER, CONSUMER,_ = set_producer_consumer(
                        kafka_config_file,
                        producer_extra_config={
                            "on_delivery": delivery_report,
                            "client.id": f"""{SYS_CONFIG['kafka-client-id']['microservice_baked']}_{HOSTNAME}""",

                        },
                        consumer_extra_config={
                            "group.id": f"""{SYS_CONFIG['kafka-client-id']['microservice_baked']}_{HOSTNAME}""",
                            "client.id": f"""{SYS_CONFIG['kafka-client-id']['microservice_baked']}_{HOSTNAME}""", # "auto.offset.reset": 'earliest',
                        },
                        disable_producer=True
                    )

graceful_shutdown = GracefulShutdown(consumer = CONSUMER)



def pizza_baked(order_id: str, bake_time: int):
    PRODUCER.produce(
        PRODUCE_TOPIC_BAKE,
        key=order_id,
        value=json.dumps(
            {
                "status": SYS_CONFIG["status-id"]["pizza_baked"],
                "timestamp": timestamp_now(),
            }
        ).encode(),
    )
    PRODUCER.flush()

def receive_pizza_assembled(order_id: str, baking_time: int):
    CONSUMER.subscribe(CONSUME_TOPICS)
    logging.info(f"Subscribed to topics: {CONSUME_TOPICS}")
    while True:
        with graceful_shutdown:
            try:
                msg = CONSUMER.poll(timeout=1.0)
                if msg is not None:
                    if msg.error():
                        log_exception(msg.error())
                    else:
                        time.sleep(0.2)
                        log_event_received(msg)
                        order = json.loads(msg.value().decode("utf-8"))
                        try:
                            baking_time = json.loads(msg.value().decode("utf-8")).get(
                                "baking_time",0
                            )
                        except Exception as e:
                            logging.error(f"Error parsing event: {e}")
                            time.sleep(0.2)
                            continue

                        if order["status"] == SYS_CONFIG["status-id"]["pizza_baked"]:
                            logging.info(f"Order {order_id} baked in {baking_time} seconds")
                            pizza_baked(order_id, baking_time)
                        elif order["status"] == SYS_CONFIG["status-id"]["pizza_assembled"]:
                            logging.info(f"Order {order_id} assembled in {baking_time} seconds")
                            pizza_baked(order_id, baking_time)

            except Exception as e:
                log_exception(e)
                time.sleep(0.2)
                
            

            CONSUMER.commit(asynchronous=False)


if __name__ == "__main__":
    # Save PID
    save_pid(SCRIPT)

    # Start consumer
    receive_pizza_assembled("123", 10)
    

                    
