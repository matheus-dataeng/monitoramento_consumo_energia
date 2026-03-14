import logging as log 

def setup_log() -> None:
    
    log.basicConfig(
        level= log.INFO,
        format= "%(asctime)s / %(levelname)s / %(name)s / %(message)s"
    )
    
    