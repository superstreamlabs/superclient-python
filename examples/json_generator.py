import random
import string
import json
from datetime import datetime

def generate_random_json(min_size_kb=1):
    """Generate a random JSON object of at least min_size_kb size"""
    base_data = {
        "timestamp": datetime.now().isoformat(),
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1000, 9999)}",
        "session_id": f"session_{random.randint(10000, 99999)}",
        "event_type": random.choice(["click", "view", "purchase", "login", "logout"]),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "country": random.choice(["US", "UK", "DE", "FR", "JP", "BR", "IN"]),
        "metrics": {
            "load_time": round(random.uniform(0.1, 5.0), 3),
            "response_time": round(random.uniform(0.01, 1.0), 3),
            "cpu_usage": round(random.uniform(0, 100), 2),
            "memory_usage": round(random.uniform(0, 100), 2)
        }
    }
    
    # Calculate current size
    current_json = json.dumps(base_data)
    current_size = len(current_json.encode('utf-8'))
    target_size = min_size_kb * 1024
    
    # Add padding data if needed to reach target size
    if current_size < target_size:
        padding_size = target_size - current_size
        # Generate random string data for padding
        padding_data = {
            "additional_data": {
                f"field_{i}": ''.join(random.choices(string.ascii_letters + string.digits, k=50))
                for i in range(padding_size // 50)
            }
        }
        base_data.update(padding_data)
    
    return base_data