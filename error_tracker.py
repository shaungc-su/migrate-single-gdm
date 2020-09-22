import json
from datetime import datetime

class ErrorTracker:
    FILENAME = f'error/error_tracker_{datetime.now().strftime("%Y%m%d-%H:%M:%S.%f")}'
    def __init__(self):
        self.error_messages = []
    
    def log(self, message):
        self.error_messages.append(message)

    def save(self):
        with open(f'{self.FILENAME}.json', 'w') as f:
            json.dump(self.error_messages, f)
        
        with open(f'{self.FILENAME}.log', 'w') as f:
            for message in self.error_messages:
                f.write(f'{message}\n')