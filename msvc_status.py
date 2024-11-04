import sys
import json
import time
import logging
from threading import Thread

# Import các hàm và lớp tiện ích từ module utils
from utils import (
    GracefulShutdown,          # Đối tượng quản lý quá trình dừng an toàn
    log_ini,                   # Khởi tạo logging
    save_pid,                  # Lưu ID tiến trình
    get_hostname,              # Lấy tên máy chủ
    log_exception,             # Ghi log ngoại lệ
    get_script_name,           # Lấy tên script
    validate_cli_args,         # Xác thực tham số dòng lệnh
    get_string_status,         # Lấy tên trạng thái đơn hàng dưới dạng chuỗi
    log_event_received,        # Ghi log khi nhận được sự kiện Kafka
    get_system_config,         # Lấy cấu hình hệ thống
    set_producer_consumer,     # Thiết lập Kafka Producer và Consumer
    import_state_store_class,  # Import lớp cơ sở dữ liệu để lưu trữ trạng thái đơn hàng
)

# Lấy tên tệp script hiện tại và tên máy chủ
SCRIPT = get_script_name(__file__)
HOSTNAME = get_hostname()

# Khởi tạo ghi log cho script
log_ini(SCRIPT)

# Lấy đường dẫn cấu hình Kafka và hệ thống từ dòng lệnh
kafka_config_file, sys_config_file = validate_cli_args(SCRIPT)

# Tải cấu hình hệ thống từ tệp
SYS_CONFIG = get_system_config(sys_config_file)

# Các Kafka topic cần tiêu thụ dữ liệu
CONSUME_TOPICS = [
    SYS_CONFIG["kafka-topics"]["pizza_status"],  # Topic trạng thái đơn hàng pizza
]

# Thiết lập Kafka Consumer
_, _, CONSUMER, _ = set_producer_consumer(
    kafka_config_file,
    disable_producer=True,  # Không cần Producer trong script này
    consumer_extra_config={
        "group.id": f"""{SYS_CONFIG["kafka-consumer-group-id"]["microservice_status"]}_{HOSTNAME}""",  # Đặt Group ID cho Consumer
        "client.id": f"""{SYS_CONFIG["kafka-client-id"]["microservice_status"]}_{HOSTNAME}""",  # Đặt ID cho Client
    },
)

# Khởi tạo GracefulShutdown để quản lý quá trình dừng an toàn của Consumer
graceful_shutdown = GracefulShutdown(consumer=CONSUMER)

# Import lớp lưu trữ trạng thái và xác định tên cơ sở dữ liệu lưu trạng thái đơn hàng
DB = import_state_store_class(SYS_CONFIG['state-store-orders']['db_module_class'])
ORDERS_DB = SYS_CONFIG['state-store-orders']['name']

# Thiết lập cơ sở dữ liệu và dọn dẹp dữ liệu cũ khi khởi động script
with graceful_shutdown as _:
    with DB(ORDERS_DB, sys_config=SYS_CONFIG) as db:
        # Tạo bảng lưu trữ đơn hàng nếu chưa tồn tại
        db.create_order_table()

        # Xóa các bản ghi đơn hàng cũ dựa vào thời gian lưu trữ được cấu hình
        db.delete_past_timestamp(
            SYS_CONFIG['state-store-orders']['table_orders'],
            hours=int(SYS_CONFIG['state-store-orders']['table_orders_retention_hours'])
        )

        # Tạo bảng trạng thái đơn hàng nếu chưa tồn tại
        db.create_status_table()

        # Xóa các bản ghi trạng thái cũ dựa vào thời gian lưu trữ được cấu hình
        db.delete_past_timestamp(
            SYS_CONFIG['state-store-orders']['table_status'],
            hours=int(SYS_CONFIG['state-store-orders']['table_status_retention_hours'])
        )

# Hàm thread_status_watchdog dùng để kiểm tra các đơn hàng bị kẹt
def thread_status_watchdog():
    # Hàm kiểm tra và cập nhật trạng thái đơn hàng kẹt
    while True:
        with DB(ORDERS_DB, sys_config=SYS_CONFIG) as db:
            # Kiểm tra trạng thái đơn hàng bị kẹt
            stuck_status = db.check_status_stuck()
            for order_id, data in stuck_status.items():
                logging.warning(f"Order {order_id} is stuck")  # Ghi log cảnh báo nếu đơn hàng bị kẹt
                # Cập nhật trạng thái đơn hàng là 'stuck'
                db.update_order_status(order_id, SYS_CONFIG['status']['stuck'])
                # Xóa trạng thái bị kẹt khỏi bảng trạng thái
                db.delete_stuck_status(order_id)
        # Thời gian giữa các lần kiểm tra là khoảng thời gian cấu hình
        time.sleep(SYS_CONFIG['state-store-orders']['status_watchdog_minutes'] * 60)

# Hàm get_pizza_status lắng nghe Kafka topic để cập nhật trạng thái đơn hàng trong cơ sở dữ liệu
def get_pizza_status():
    """Subscribe vào topic pizza-status để cập nhật cơ sở dữ liệu tạm thời (order_ids dict)"""
    CONSUMER.subscribe(CONSUME_TOPICS)  # Đăng ký vào các Kafka topic cần tiêu thụ
    logging.info(f"Subscribed to topic(s): {', '.join(CONSUME_TOPICS)}")

    # Vòng lặp lắng nghe sự kiện Kafka
    while True:
        with graceful_shutdown as _:
            event = CONSUMER.poll(1)  # Chờ và nhận sự kiện từ Kafka

            if event is not None:
                if event.error():  # Kiểm tra lỗi từ sự kiện
                    logging.error(event.error())
                else:
                    try:
                        log_event_received(event)  # Ghi log khi nhận sự kiện

                        order_id = event.key().decode()  # Giải mã khóa đơn hàng từ Kafka event

                        with DB(ORDERS_DB, sys_config=SYS_CONFIG) as db:
                            # Lấy dữ liệu đơn hàng từ cơ sở dữ liệu
                            order_data = db.get_order_id(order_id)

                            if order_data is not None:
                                try:
                                    # Giải mã và lấy trạng thái pizza từ nội dung Kafka event
                                    pizza_status = json.loads(event.value().decode()).get(
                                        "STATUS",
                                        SYS_CONFIG["status-id"]["unknown"],
                                    )
                                except Exception:
                                    # Xử lý ngoại lệ khi không lấy được trạng thái
                                    pizza_status = SYS_CONFIG["status-id"]["something_wrong"]
                                    log_exception(
                                        f"Error when processing event.value() {event.value()}",
                                        sys.exc_info(),
                                    )
                                finally:
                                    # Ghi log trạng thái mới của đơn hàng
                                    logging.info(
                                        f"""Order '{order_id}' status updated: {get_string_status(SYS_CONFIG["status"], pizza_status)} ({pizza_status})"""
                                    )
                                    # Cập nhật trạng thái đơn hàng trong cơ sở dữ liệu
                                    db.update_order_status(order_id, pizza_status)
                                    # Thêm trạng thái vào bảng trạng thái
                                    db.upsert_status(order_id, pizza_status)

                                    # Xóa trạng thái khỏi bảng trạng thái nếu đơn hàng đã kết thúc hoặc gặp lỗi
                                    if int(pizza_status) in (
                                        SYS_CONFIG["status-id"]["stuck"],
                                        SYS_CONFIG["status-id"]["cancelled"],
                                        SYS_CONFIG["status-id"]["delivered"],
                                        SYS_CONFIG["status-id"]["something_wrong"],
                                        SYS_CONFIG["status-id"]["unknown"],
                                    ):
                                        db.delete_stuck_status(order_id)
                            else:
                                logging.error(f"Order '{order_id}' not found")  # Log lỗi nếu không tìm thấy đơn hàng
                    except Exception:
                        # Log ngoại lệ nếu có lỗi trong quá trình xử lý khóa sự kiện
                        log_exception(
                            f"Error when processing event.key() {event.key()}",
                            sys.exc_info(),
                        )

                # Commit offset một cách thủ công để Kafka biết sự kiện đã được xử lý
                CONSUMER.commit(asynchronous=False)

########
# Main #
########
if __name__ == "__main__":
    # Lưu PID của tiến trình hiện tại để dễ dàng theo dõi và quản lý
    save_pid(SCRIPT)

    # Khởi động luồng kiểm tra trạng thái bị kẹt của đơn hàng
    Thread(target=thread_status_watchdog, daemon=True).start()

    # Bắt đầu quá trình lắng nghe trạng thái đơn hàng
    get_pizza_status()
