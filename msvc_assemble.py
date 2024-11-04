import sys
import json
import time
import hashlib
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
PRODUCE_TOPIC_STATUS = SYS_CONFIG['kafka-topics'].get('pizza_status')
PRODUCE_TOPIC_ASSEMBLED = SYS_CONFIG['kafka-topics']['pizza_assembled']
CONSUME_TOPICS = [SYS_CONFIG['kafka-topics']['pizza_ordered']]

_, PRODUCER, CONSUMER, _ = set_producer_consumer(
    kafka_config_file,
    producer_extra_config={
        "on_delivery": delivery_report,
        "client.id": f"{SYS_CONFIG['kafka-client-id']['microservice_assembled']}_{HOSTNAME}",
    },
    consumer_extra_config={
        "group.id": f"{SYS_CONFIG['kafka-consumer-group-id']['microservice_assembled']}_{HOSTNAME}",
        "client.id": f"{SYS_CONFIG['kafka-client-id']['microservice_assembled']}_{HOSTNAME}",
    }
)

GRACEFUL_SHUTDOWN = GracefulShutdown(consumer=CONSUMER)

def pizza_assembled(order_id: str, baking_time: int):
    PRODUCER.produce(
        PRODUCE_TOPIC_ASSEMBLED,
        key=order_id,
        value=json.dumps({
            "status": SYS_CONFIG["status-id"]["pizza_assembled"],
            "baking_time": baking_time,
            "timestamp": timestamp_now(),
        }).encode()
    )
    PRODUCER.flush()

def receive_orders():
    # Đăng ký Consumer để nhận các sự kiện từ topic được chỉ định trong CONSUME_TOPICS
    """
    Continuously receives and processes orders from a Kafka topic.

    This function subscribes a Kafka consumer to the topics specified in
    CONSUME_TOPICS. It enters an infinite loop to poll for new events from the
    Kafka topic, handling each event safely using the GracefulShutdown context
    manager. If an event is received, it checks for errors and logs them. For
    valid events, it decodes the event key and value to extract order details,
    computes assembly and baking times, logs the process, and sends a message
    to the 'pizza_assembled' Kafka topic when the assembly is complete.

    Utilizes:
        - GracefulShutdown: for safe shutdown handling.
        - Kafka Consumer: to receive events.
        - Kafka Producer: to send assembled pizza status.
        - Logging: for error and process logging.

    Raises:
        Exception: If there is an error decoding the event or processing the
        order details.
    """
    CONSUMER.subscribe(CONSUME_TOPICS)
    logging.info(f"Subscribed to topics: {CONSUME_TOPICS}")

    # Vòng lặp liên tục để Consumer kiểm tra và xử lý các sự kiện từ Kafka
    while True:
        # Sử dụng GracefulShutdown để xử lý tín hiệu dừng an toàn khi cần thiết
        with GRACEFUL_SHUTDOWN as _:
            # Kiểm tra xem có sự kiện nào mới trong Kafka topic không
            event = CONSUMER.poll(1)  # Đợi tối đa 1 giây để nhận sự kiện

            # Nếu có sự kiện (không rỗng)
            if event is not None:
                # Kiểm tra xem sự kiện có lỗi không
                if event.error():
                    # Nếu có lỗi, ghi lại thông báo lỗi vào log để gỡ lỗi
                    logging.error(event.error())
                else:
                    # Xử lý sự kiện hợp lệ
                    try:
                        # Thêm độ trễ ngắn để cho các bản ghi từ microservice khác hiển thị trước
                        time.sleep(0.15)  # Để dễ dàng theo dõi log

                        # Ghi log sự kiện vừa nhận để kiểm tra thông tin đơn hàng
                        log_event_received(event)

                        # Giải mã key của sự kiện để lấy mã đơn hàng (order_id)
                        order_id = event.key().decode()
                        
                        # Giải mã và giải nén dữ liệu JSON của sự kiện để lấy chi tiết đơn hàng
                        try:
                            # `order_details` chứa thông tin chi tiết của đơn hàng
                            order_details = json.loads(event.value().decode())
                            # `order` lấy các thông tin cụ thể của đơn hàng (các thành phần của pizza)
                            order = order_details.get("order", dict())
                        except Exception:
                            # Ghi log nếu có lỗi khi giải mã hoặc giải nén JSON
                            log_exception(
                                f"Error when processing event.value() {event.value()}",
                                sys.exc_info(),
                            )

                        else:
                            # Tạo giá trị `seed` ngẫu nhiên từ các thành phần của đơn hàng bằng cách hash MD5
                            seed = int(
                                hashlib.md5(
                                    # Kết hợp các thành phần của pizza để tạo chuỗi unique cho đơn hàng
                                    f"{order['sauce']}@{order['cheese']}@{','.join(order['extra_toppings'])}@{order['main_topping']}".encode()
                                ).hexdigest()[-4:],  # Lấy 4 ký tự cuối của mã hash
                                16  # Đổi thành số nguyên dạng hệ 16 (hexadecimal)
                            )

                            # Tính toán thời gian lắp ráp (assembling_time) dựa trên `seed`
                            assembling_time = seed % 8 + 4  # Kết quả sẽ nằm trong khoảng 4-11 giây
                            
                            # Ghi log về thời gian lắp ráp và mã đơn hàng hiện tại
                            logging.info(
                                f"Preparing order '{order_id}', assembling time is {assembling_time} second(s)"
                            )

                            # Dừng chương trình trong thời gian `assembling_time` để giả lập quá trình lắp ráp
                            time.sleep(assembling_time)

                            # Ghi log xác nhận pizza đã hoàn thành lắp ráp
                            logging.info(f"Order '{order_id}' is assembled!")

                            # Tính toán thời gian nướng bánh dựa trên `seed`
                            baking_time = seed % 8 + 8  # Thời gian sẽ nằm trong khoảng 8-15 giây
                            
                            # Gửi thông tin hoàn thành lắp ráp vào Kafka qua topic pizza_assembled
                            pizza_assembled(order_id, baking_time)

                    except Exception:
                        # Ghi log nếu có lỗi khi xử lý key của sự kiện
                        log_exception(
                            f"Error when processing event.key() {event.key()}",
                            sys.exc_info(),
                        )

                # Xác nhận sự kiện đã xử lý xong (commit offset) để Kafka không gửi lại
                CONSUMER.commit(asynchronous=False)



########
# Main #
########
if __name__ == "__main__":
    # Save PID
    save_pid(SCRIPT)

    # Start consumer
    receive_orders()


# +-------------------+      +-----------------------+       +--------------------+
# |                   |      |                       |       |                    |
# |  Kafka Topic:     | ---> | Microservice Pizza    | ----> | Kafka Topic:       |
# |  pizza_ordered    |      | (Assemble Orders)     |       | pizza_assembled    |
# |                   |      |                       |       |                    |
# +-------------------+      +-----------------------+       +--------------------+

