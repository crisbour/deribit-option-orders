import asyncio
import websockets
import json
from datetime import datetime
import time
import logging
import threading
import hashlib
import hmac
import queue
import secrets
import bisect

''' ---- Wrapper for creating a new thread for a functionn ----'''

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper


''' ---- Object of available instruments ---- '''

class Instruments:
    ''' Instruments object from public/get_instruments '''
    _fields = ['instrument_name', 'expiration_timestamp', 'strike', 'option_type']

    def __init__(self, time_interval = 86400, currency='ETH'):
        '''
        Parameters
        ----------
        time_interval:  int, optional
            Interval of time between refreshing instruments
        currency:       str, 'ETH'/'BTC'
            Select what currency you want to request instruments for
        '''
        self.refresh_interval = time_interval
        self.instruments = []
        self.unique_expiry_time = []
        self._lock = threading.Lock()
        self.feed_thread = self.feed(currency)

    def __del__ (self):
        self.feed_thread.join()
    
    @threaded
    def feed(self, currency):
        ''' Threaded routine to feed the instruments in the object '''
        asyncio.run(self._feed(currency))

    async def _feed(self, currency):
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
        # websocket querry for get_instruments
        while True:
            self._lock.acquire()
            logging.info('Requesting public/get_instruments for {}')
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                await websocket.send(json.dumps(msg))
                if websocket.open:
                    response = await websocket.recv()

            # Process received message and store values of interest
            self.extract(json.loads(response))
            logging.info('Successfully updated list of instruments')

            self._lock.release()
            # Wait a day (or user defined interval) to refresh interval
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
        
        # Lock access to object until writing the new data
        self.instruments = local_instruments
        self.unique_expiry_time = local_unique_expiry_time
        print('Unique expiration timestamps: {}'.format(self.unique_expiry_time))
        # Release the lock


    def get_instruments(self, expirytime, strike_price, type):
        ''' Return the instruments that match best with the given parameters.
            Parameters
            ----------
            expirytime:     int
            strike_price:   float
            type:           str ('put'/'call')
        '''
        # Wait in case instruments have not been loaded for the first time yet
        while len(self.instruments) == 0:
            pass

        logging.info('Looking for instrument to match (expirytime, strike_price, type)=({},{},{})'.format(expirytime, strike_price, type))

        # Verify expirytime is valid
        assert expirytime > int(time.time()*1000), "Expirytime passed {} miliseconds ago".format(expirytime-int(time.time()*1000))

        # Wait for feeding to finish
        self._lock.acquire()
        # Find instruments with nearest expiry time
        distance = lambda a, b: (a - b) if (a >= b) else float('inf')
        expiration_timestamp = min(self.unique_expiry_time, key = lambda x: distance(x, expirytime))

        # Filter instruments with found expiration timestamp and given type
        trim_instruments = list(filter(lambda x: x[self._fields.index('expiration_timestamp')] == expiration_timestamp and \
            x[self._fields.index('option_type')] == type, 
            self.instruments))
        
        # Give feeding back control
        self._lock.release()

        # Find nearest strike
        instrument = min(trim_instruments, key = lambda x: abs(strike_price - x[self._fields.index('strike')]))

        return tuple(instrument[self._fields.index(field)] for field in ['instrument_name', 'expiration_timestamp', 'strike'])


logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                            datefmt="%H:%M:%S")

instruments = Instruments(time_interval=50, currency='ETH')

while True:
    print(instruments.get_instruments(int(time.time()*1000+10000000), 1300, 'put'))
    time.sleep(20)