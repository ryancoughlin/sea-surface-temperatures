from typing import Dict
from .base_service import BaseService
from .erddap_service import ERDDAPService
from .podaac_service import PODAACService

class ServiceFactory:
    @staticmethod
    def get_service(source_type: str) -> BaseService:
        services = {
            'erddap': ERDDAPService(),
            'podaac': PODAACService()
        }
        
        if source_type not in services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        return services[source_type]