from abc import ABC, abstractmethod

class BaseStateStore(ABC):
    @abstractmethod
    def create_customer_table(
        self,
        *args,
        **kwargs
    ):
        pass


    @abstractmethod
    def create_order_table(
        self,
        *args,
        **kwargs
    ):
        pass

    @abstractmethod
    def create_status_table(
        self,
        *args,
        **kwargs
    ):
        pass
    
    @abstractmethod
    def check_status_stuck(
        self,
        *args,
        **kwargs
    ):
        pass

    @abstractmethod
    def delete_stuck_status(
        self,
        order_id:str,
        *args,
        **kwargs
    ):
        pass

    @abstractmethod
    def delete_past_timestamp(
        self,
        table_name:str,
        *args,
        timestamp_field:str = "timestamp",
        hours:int = 1,
        **kwargs
    ):
        pass

    @abstractmethod
    def get_order_id(
        self,
        order_id:str,
        *args,
        customer_id:str = None,
        **kwargs
    ) -> dict:
        pass


    @abstractmethod
    def get_orders(
        self,
        customer_id:str,
        *args,
        **kwargs
    ) -> dict:
        pass

    @abstractmethod
    def update_order_status(
        self,
        order_id:str,
        status:str,
        *args,
        **kwargs
    ):
        pass


    @abstractmethod
    def upsert_status(
        self,
        order_id:str,
        status:str,
        *args,
        **kwargs
    ):
        pass


    @abstractmethod
    def update_customer(
        self,
        order_id:str,
        customer_id:str,
        *args,
        **kwargs
    ):
        pass


    @abstractmethod
    def add_customer(
        self,
        order_id:str,
        customer_id:str,
        *args,
        **kwargs
    ):
        pass


    @abstractmethod
    def add_order(
        self,
        order_id:str,
        order_details: dict,
        *args,
        **kwargs
    ):
        pass
