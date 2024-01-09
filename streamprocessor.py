from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import asyncio
import time
import pandas as pd
import yfinance as yf
import sys
import json

class StreamProcessor():
    def send_data():
        async def run():

            while True:
                await asyncio.sleep(5)
                producer = EventHubProducerClient.from_connection_string(conn_str='Endpoint=sb://stockstreaminghub.servicebus.windows.net/;SharedAccessKeyName=getdata;SharedAccessKey=gyyVdiNYqJiLw9ybEbyGhfV8x4MjLbsVl+AEhKuGHb4=', eventhub_name='getdata')
                async with producer:
                    
                    event_data_batch = await producer.create_batch()

                    event_data_batch.add(EventData(json.dumps({"hey" : "Hello world"})))

                    await producer.send_batch(event_data_batch)
                    print("Data sent successfully to eventhubs")

        loop = asyncio.get_event_loop()

        try:
            asyncio.ensure_future(run())
            loop.run_forever()
        except KeyboardInterrupt:
            print("Keyboard interrupted")
            loop.close()
        finally:
            print("Closing loop now")
            loop.close()

StreamProcessor.send_data()

