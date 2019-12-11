from dependency_injector import providers, containers
from logger import WorkerLogger

class Dependencies(containers.DeclarativeContainer):
    log = providers.Singleton(WorkerLogger)