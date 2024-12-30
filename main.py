import os
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import json
from datetime import datetime
import time
import redis
import warnings

# Ignorar todas las advertencias
warnings.filterwarnings("ignore")

class DataExtractor:
    def __init__(self, endpoint_url=None):
        url = os.getenv("base_url", "")
        city = os.getenv("city", "")
        api_key = os.getenv("api_key", "")
        self.endpoint_url = f"{url}{city}.proto?apikey={api_key}"
        pass

        # Función para obtener los datos GTFS-RT
    def get_gtfs_rt(self):
        try:
            response = requests.get(self.endpoint_url)
            response.raise_for_status()  # Verifica que no haya errores en la solicitud
            self.proto_data = response.content
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos GTFS-RT: {e}")
            return None

    # Función para parsear los datos GTFS-RT
    def gtfs_rt_to_json(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(self.proto_data)
        # Convertir a JSON para visualizar mejor
        feed_json = MessageToJson(feed)
        self.feed_json = json.loads(feed_json)
        pass
    
    def fetch_data(self):
        try:
            self.get_gtfs_rt()
            self.gtfs_rt_to_json()
            self.data = self.feed_json
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener los datos: {e}")
            self.data = False
        pass
        
    def set_current_timestamp(self):
        self.timestamp = datetime.now()
        self.timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        pass

    def connect_to_redis(self, host='redis-gtfs', port=6379, db=8):
        try:
            self.redis_client = redis.StrictRedis(host=host, port=port, db=db)
            print("Conexión a Redis establecida.")
        except redis.ConnectionError as e:
            print(f"No se pudo conectar a Redis: {e}")

    def save_data_to_redis(self):
        if self.redis_client:
            try:
                self.redis_client.set(self.timestamp_str, json.dumps(self.data))
                print(f"Datos guardados en Redis con clave '{self.timestamp_str}'")
            except redis.RedisError as e:
                print(f"Error al guardar los datos en Redis: {e}")
    
    def disconnect_from_redis(self):
        if self.redis_client:
            self.redis_client.close()
            print("Conexión a Redis cerrada.")

def main():
    data_extractor = DataExtractor()
    print(f"URL de la API: {data_extractor.endpoint_url}")
    data_extractor.connect_to_redis()

    while(True):
        data_extractor.set_current_timestamp()
        print(f"Timestamp actual: {data_extractor.timestamp}")
    
        for i in range(5):
            print(f"Intento {i+1}")
            try:
                data_extractor.fetch_data()
                if data_extractor.data:
                    print(f"Datos obtenidos")
                    break
                else:
                    print("No se pudieron obtener los datos.")
            except Exception as e:
                print(f"Error al obtener los datos: {e}")
                time.sleep(1)

        data_extractor.save_data_to_redis()
        time.sleep(60)

    data_extractor.disconnect_from_redis()
    print("Proceso finalizado.")
    pass

if __name__ == "__main__":
    main()