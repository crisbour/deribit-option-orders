import asyncio
import websockets
import json
import bisect
from datetime import datetime
import time
import logging

class Instruments:
    ''' Instruments object from public/get_instruments '''
    _fields = ['instrument_name', 'expiration_timestamp', 'strike', 'option_type']

    def __init__(self, time_interval = 86400):
        '''
        Parameters
        ----------
        time_interval : int, optional
            Interval of time between refreshing instruments
        '''
        self.refresh_interval = time_interval
        self.instruments = []
        self.unique_expiry_time = []

    async def feed(self, currency = "ETH"):
        assert currency in ["ETH", "BTC"]
        # Construct json message needed for the querry
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 1,
        "method" : "public/get_instruments",
        "params" : {
            "currency" : currency,
            "kind" : "option",
            "expired" : False
            }
        }
        while True:
            # websocket querry for get_instruments
            logging.info('Requesting public/get_instruments for {}')
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                await websocket.send(json.dumps(msg))
                while websocket.open:
                    response = await websocket.recv()
                    await websocket.close()

            # Process received message and store values of interest
            self.extract(json.loads(response))
            logging.info('Successfully updated list of instruments')

            # Wait a day (or user defined interval) to refresh interval
            print(self.get_instruments(int(time.time()*1000+10000000), 1300, 'put'))
            await asyncio.sleep(self.refresh_interval)

    def extract(self, response):
        ''' Extract data about instruments from json message. 
            The message needs to be converted to python dictionary first with json.loads(msg).
            Parameters
            ----------
            response: str - Message
        '''
        logging.info('Extract and sort querried instruments')
        local_instruments = []
        local_unique_expiry_time = []
        for elem in response['result']:
            local_instruments.append(tuple(elem[field] for field in self._fields))
            if elem['expiration_timestamp'] not in local_unique_expiry_time:
                bisect.insort(local_unique_expiry_time, elem['expiration_timestamp'])

        # Sort the instruments data to ease looking for the right instrument
        local_instruments.sort(key = lambda x : (x[self._fields.index('expiration_timestamp')], x[self._fields.index('strike')]))
        # for instr in local_instruments:
        #     print(instr)
        
        # Semaphore block access to object until writing the new data
        self.instruments = local_instruments
        self.unique_expiry_time = local_unique_expiry_time
        print('Unique expiration timestamps: {}'.format(self.unique_expiry_time))
        # Unblock semaphore

    def get_instruments(self, expirytime, strike_price, type):
        ''' Return the instruments that match best with the given parameters.
            Parameters
            ----------
            expirytime:     int
            strike_price:   float
            type:           str ('put'/'call')
        '''
        logging.info('Looking for instrument to match (expirytime, strike_price, type)=({},{},{})'.format(expirytime, strike_price, type))

        # Verify expirytime is valid
        assert expirytime > int(time.time()*1000), "Expirytime passed {} miliseconds ago".format(expirytime-int(time.time()*1000))
        
        # Find instruments with nearest expiry time
        distance = lambda a, b: (a - b) if (a >= b) else float('inf')
        expiration_timestamp = min(self.unique_expiry_time, key = lambda x: distance(x, expirytime))

        # Filter instruments with found expiration timestamp and given type
        trim_instruments = list(filter(lambda x: x[self._fields.index('expiration_timestamp')] == expiration_timestamp and \
            x[self._fields.index('option_type')] == type, 
            self.instruments))

        # Find nearest strike
        instrument = min(trim_instruments, key = lambda x: abs(strike_price - x[self._fields.index('strike')]))

        return tuple(instrument[self._fields.index(field)] for field in ['instrument_name', 'expiration_timestamp', 'strike'])

class WebsocketEngine:
    def __init__(self):
        pass


class OptionsOrder:
    orders_id = []
    websocket_flags = 0
    def __init__(self):
        pass
    async def _call_api(self):
        async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        ###############
        # Before sending message, make sure that your connection
        # is authenticated (use public/auth call before) 
        ###############
            await websocket.send(msg)
            while websocket.open:
                response = await websocket.recv()
                # do something with the response...
                print(response)


instruments = Instruments(time_interval=50)
# instruments.feedInstruments()
#asyncio.get_event_loop().run_until_complete(instruments.feedInstruments())
logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")
asyncio.run(instruments.feed())
