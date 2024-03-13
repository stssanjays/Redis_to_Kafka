import redis
from kafka import KafkaProducer
import json

redis_host = ''
redis_port = 
redis_password = ''

# Kafka configuration
kafka_bootstrap_servers = ''  # Replace with your Kafka broker's address and port

redis_client = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
# producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

cursor = '0'
hash_key_count = 0
hash_keys = {}  # Using a dictionary to store key hashes and values
previous_json_data = {}

def flatten_json(nested_json, parent_key='', sep='_'):
    items = {}
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, dict):
                items.update(flatten_json(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    items.update(flatten_json(v, new_key + sep + str(i), sep=sep).items())
            else:
                items[new_key] = value
    elif isinstance(nested_json, list):
        for i, value in enumerate(nested_json):
            items.update(flatten_json(value, parent_key + sep + str(i), sep=sep).items())
    else:
        items[parent_key] = nested_json
    return items

while True:
    cursor, keys = redis_client.scan(cursor, count=10)
    for key in keys:
        try:
            if key not in hash_keys or hash_keys[key] != redis_client.hgetall(key):  # Check if key is not processed or updated
                key_type = redis_client.type(key)

                if key_type == 'hash':
                    print("Key Name:", key, "Type: Hash")
                    data = redis_client.hgetall(key)
                    for k, v in data.items():
                        try:
                            data[k] = json.loads(v)
                        except json.JSONDecodeError:
                            pass

                    # Convert the nested data into JSON
                    json_data = json.dumps(data, indent=4)

                    # Convert the JSON string back into a dictionary
                    output_dict = json.loads(json_data)

                    # Flatten nested JSON
                    flattened_json = flatten_json(output_dict)
                    # Convert to JSON string
                    normal_json_str = json.dumps(flattened_json, indent=2)
                    # print(type(normal_json_str))

                    kafka_hash = json.loads(normal_json_str)
                    # print(kafka_hash)
                    hash_keys[key] = redis_client.hgetall(key)  # Update hash for the key
                    producer.send('redis_hash_update', value=kafka_hash)
                    # print("Data sent to Kafka for hash key:", key)


                elif key_type == 'ReJSON-RL':
                    try:
                        json_data = redis_client.execute_command('JSON.GET', key)
                        if json_data and (json_data != previous_json_data.get(key, None)):
                            print("Key Name:", key, "Type: Json")
                            decoded_json = json.loads(json_data)
                            # print(type(decoded_json))
                            # print(json.loads(json_data))
                            previous_json_data[key] = json_data
                            kafka_json = json.loads(json_data)
                            # print(type(kafka_json))
                            producer.send('redis_json_update', value=kafka_json)
                    except redis.exceptions.ResponseError:
                        continue
                hash_key_count += 1
        except redis.exceptions.ResponseError as e:
            continue

    if cursor == '0':
        break

producer.flush()
producer.close()
