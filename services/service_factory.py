from typing import Dict
from .erddap_service import ERDDAPService
from .podaac_service import PODAACService
from .cmems_service import CMEMSService

class ServiceFactory:
    @staticmethod
    def get_service(source_type: str) -> BaseService:
        services = {
            'erddap': ERDDAPService(),
            'podaac': PODAACService(),
            'cmems': CMEMSService()
        }
        
        if source_type not in services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        return services[source_type]