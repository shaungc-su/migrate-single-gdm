from time import perf_counter
from datetime import timedelta

class _Logger:
    LOG_VALUES = {
        'DEBUG': 3,
        'INFO': 2,
        'WARNING': 1,
        'ERROR': 0,
    }

    LOG_LEVEL = LOG_VALUES['DEBUG']

    DEFAULT_MSG_TRUNCATE = 1000
    
    def debug(self, msg):
        print(f'🐛 DEBUG: {msg[:self.DEFAULT_MSG_TRUNCATE]}...') if self.LOG_LEVEL >= self.LOG_VALUES['DEBUG'] else None
    def info(self, msg):
        print(f'💬 INFO: {msg[:self.DEFAULT_MSG_TRUNCATE]}...') if self.LOG_LEVEL >= self.LOG_VALUES['INFO'] else None
    def warn(self, msg):
        print(f'🟠 WARN: {msg[:self.DEFAULT_MSG_TRUNCATE]}...') if self.LOG_LEVEL >= self.LOG_VALUES['WARNING'] else None
    def error(self, msg):
        print(f'🔴 ERROR: {msg[:self.DEFAULT_MSG_TRUNCATE]}...') if self.LOG_LEVEL >= self.LOG_VALUES['ERROR'] else None
    def persist(self, msg):
        with open(f'./.data/message.log', 'a+') as f:
            f.write(msg)

Logger = _Logger()

class Estimate:
    def __init__(self):
        self.start = perf_counter()
    
    def get(self, progress_value: int, total_value: int) -> (int, timedelta):
        '''Provides estimate for remaining time to complete.
            Refer to https://stackoverflow.com/a/50189329/9814131
        '''
        stop = perf_counter()
        eta_total_seconds = round((stop - self.start) * (total_value / progress_value)) if progress_value else float('INF')
        progress_percent = 100 * progress_value // total_value

        elapsed_seconds = round(stop - self.start)
        eta_remaining_seconds = eta_total_seconds - elapsed_seconds
        eta_time_delta = timedelta(seconds=eta_remaining_seconds)
        
        return progress_percent, eta_time_delta