class Logger:
    LOG_VALUES = {
        'DEBUG': 3,
        'INFO': 2,
        'WARNING': 1,
        'ERROR': 0,
    }

    LOG_LEVEL = LOG_VALUES['DEBUG']
    
    def debug(self, msg):
        print(f'🐛 DEBUG: {msg}') if self.LOG_LEVEL >= self.LOG_VALUES['DEBUG'] else None
    def info(self, msg):
        print(f'💬 INFO: {msg}') if self.LOG_LEVEL >= self.LOG_VALUES['INFO'] else None
    def warn(self, msg):
        print(f'🟠 WARN: {msg}') if self.LOG_LEVEL >= self.LOG_VALUES['WARNING'] else None
    def error(self, msg):
        print(f'🔴 ERROR: {msg}') if self.LOG_LEVEL >= self.LOG_VALUES['ERROR'] else None