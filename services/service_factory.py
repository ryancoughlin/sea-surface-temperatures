from typing import Dict
from .erddap_service import ERDDAPService
from .cmems_service import CMEMSService
from .base_service import BaseService

class ServiceFactory:
    @staticmethod
    def get_service(source_type: str) -> BaseService:
        services = {
            'erddap': ERDDAPService(),
            'cmems': CMEMSService()
        }
        
        if source_type not in services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        return services[source_type]