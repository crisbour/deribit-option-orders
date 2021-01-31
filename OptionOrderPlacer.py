import asyncio
import websockets
import json
from datetime import datetime, timedelta
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

    def __init__(self, client_websocket, time_interval = 86400, currency='ETH'):
        '''
        Parameters
        ----------
        time_interval:  int, optional
            Interval of time between refreshing instruments
        currency:       str, 'ETH'/'BTC'
            Select what currency you want to request instruments for
        '''
        self.client_websocket = client_websocket
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
        assert currency in ["ETH", "BTC"]

        while True:
            logging.info('Requesting public/get_instruments for {}')
            response = self.client_websocket.feed_instruments(currency)

            # Lock thread
            self._lock.acquire()
            # Process received message and store values of interest
            self.extract(response)
            logging.info('Successfully updated list of instruments')

            self._lock.release()
            
            time.sleep(self.refresh_interval)
        # asyncio.run(self._feed(currency))

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

class DeribitExhchangeVersion:
    def __init__(self, exchange_version):
        if exchange_version == 'live':
            self.exchange_version = 'wss://www.deribit.com/ws/api/v2/'
        elif exchange_version == 'testnet':
            self.exchange_version = 'wss://test.deribit.com/ws/api/v2/'
        else:
            logging.error('Invalide Exchange Version, please try "live" or "testnet"')
    def get(self):
        return self.exchange_version

''' ---- Create client signature for secure authentification ---- '''
class ClientSignature:
    def __init__(self, clientId, clientSecret, data=''):
        self.clientId = clientId
        tstamp = round(datetime.now().timestamp() * 1000)
        nonce = secrets.token_urlsafe(10)
        base_signature_string = str(tstamp) + "\n" + nonce + "\n" + data
        byte_key = clientSecret.encode()
        message = base_signature_string.encode()
        signature = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
        self.client_signature = {"client_id": clientId,"timestamp": tstamp, "signature": signature, "nonce": nonce, "data": data}

    def get_client_signature(self):
        return self.client_signature

''' ---- Schedule websocket authentification, querries and information receiving ---- '''
class ClientWebsocket:
    ''' Client Websocket to interact with the server, pulling and pushing json messages'''

    # Configurations constants
    REFRSH_TOKEN   =   0x01     # Refresh token flgas
    AUTH_REFRESH    =   0x02    # Authentification refresh flag
    HEART_REQ       =   0x04    # Heartbeat requested flag
    HEART_SET       =   0x08    # Heartbeat set flag
    class Flags:
        def __init__(self):
            self.flags = 0x00
        def check(self, mask):
            return self.flags & mask
        def change(self, mask, val):
            self.flags = (self.flags | mask) if val else (self.flags ^ mask)

    def __init__(self, client_signature, exchange_version = 'testnet'):
        ''' Set up a websocket for talking with the api.
            Parameters
            ----------
            client_signature:   ClientSignature
            exchange_version:   str = 'testnet'/'live'
        '''
        self.scope = "account:read_write trade:read_write wallet:read_write block_trade:read_write custody:read_write"
        self.msg_out = queue.Queue()
        self.instruments = queue.Queue()
        self.orderbook = queue.Queue()
        self.order_state = queue.Queue()
        self.buy_reply = queue.Queue()
        self.id2queue = {1: None, 2: self.orderbook, 3: self.buy_reply, 4: self.order_state, 5: self.instruments}
        self.flags = self.Flags()
        self.refersh_token = ''
        self.client_auth(client_signature)
        self.heartbeat()
        self._lock = threading.Lock()
        self.send_read_thread = self.websocket_worker()
    
    def __del__(self):
        self.send_read_thread.join()

    @threaded 
    def websocket_worker(self):
        asyncio.run(self._send_receive())

    
    async def _send_receive(self):
        msg_in = asyncio.Queue()
        self.connection_state = asyncio.Queue()
        self.id2queue[1] = self.connection_state

        await asyncio.gather(self._socket(msg_in),self._receive(msg_in), self._maintain_connection())


    async def _receive(self, msg_in):
        logging.info('Interpret received messages')
        while True:
            msg = await msg_in.get()
            msg = json.loads(msg)

            if 'error' in msg.keys():
                error_message = msg['error']['message']
                error_code = msg['error']['code']
                logging.error('You have received an ERROR MESSAGE: {} with the ERROR CODE: {}'.format(error_message, error_code))

            try:
                if msg['id'] in self.id2queue.keys():
                    q = self.id2queue[msg['id']]
                    if msg['id'] == 1:
                        await q.put(msg)
                    else:
                        q.put(msg)
            except:
                await self.id2queue[1].put(msg)

    async def _socket(self, msg_in):
        async def send(websocket):
            if(not self.msg_out.empty()):
                msg = self.msg_out.get()
                logging.debug('Sending')
                await websocket.send(msg)
                logging.debug(msg)
        async def receive(websocket):
            if(websocket.open):
                response = await websocket.recv()
                if response:
                    logging.debug('Receiving')
                    await msg_in.put(response)
                    logging.debug(response)

        async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
            while True:
                await asyncio.gather(send(websocket),receive(websocket))
                
    async def _maintain_connection(self):
        while True:
            msg = await self.connection_state.get()
            if 'result' in msg.keys():
                if [*msg['result']] == ['token_type', 'scope', 'refresh_token', 'expires_in', 'access_token']:            
                    if self.flags.check(self.AUTH_REFRESH):
                        logging.info('Successfully Refreshed your Authentication')
                    else:
                        logging.info('Authentication Success')                
                    self.refresh_token = msg['result']['refresh_token']
                    if msg['testnet']:
                        # The testnet returns an extremely high value for expires_in and is best to use                       
                        # 600 in place as so the functionality is as similar as the Live exchange                            
                        expires_in = 600
                    else:
                        expires_in = msg['result']['expires_in']

                    self.expiry_time = (datetime.now() + timedelta(seconds=expires_in))                                     
                    logging.info('Authentication Expires at: ' + str(self.expiry_time.strftime('%H:%M:%S')))
            
            # Use refresh_token to refresh authentification
            if datetime.now() > self.expiry_time and not self.flags.check(self.AUTH_REFRESH):                     
                self.flags.change(self.AUTH_REFRESH, 1)
                logging.info('Refreshing your Authentication')              
                ws_data = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "public/auth",
                    "params": {
                        "grant_type": "refresh_token",
                        "refresh_token": self.refresh_token
                    }
                }
                self.msg_out.put(json.dumps(ws_data))
            
            # Heartbeat set success check and heartbeat response
            if 'params' in msg.keys() and msg['params']['type'] == 'heartbeat' and not self.flags.check(self.HEART_SET): 
                self.flags.change(self.HEART_SET, 1)
                logging.info('Heartbeat Successfully Initiated ')

            # Respond to a test request
            if 'params' in msg.keys() and msg['params']['type'] == 'test_request':                                    # noqa: E501
                ws_data = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "public/test",
                    "params": {
                    }
                }
                self.msg_out.put(json.dumps(ws_data))


    def client_auth(self, client_signature):
        logging.info('Authentification starting')
        signature = client_signature.get_client_signature()
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/auth",
            "params": {
                "grant_type": "client_signature",
                "client_id": signature['client_id'],
                "timestamp": signature['timestamp'],
                "signature": signature['signature'],
                "nonce": signature['nonce'],
                "scope": self.scope,
                "data": signature['data']
            }
        }
        self.msg_out.put(json.dumps(msg))
    
    def heartbeat(self):
        # Initiating Hearbeat
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/set_heartbeat",
            "params": {
                "interval": 10
            }
        }
        self.msg_out.put(json.dumps(msg))
    
    def feed_instruments(self, currency):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 5,
        "method" : "public/get_instruments",
        "params" : {
            "currency" : currency,
            "kind" : "option",
            "expired" : False
            }
        }
        self.msg_out.put(json.dumps(msg))
        while self.instruments.empty():
            pass
        return self.instruments.get()

    def get_orderbook(self, instrument_name):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 2,
        "method" : "public/get_book_summary_by_instrument",
        "params" : {
            "instrument_name" : instrument_name
        }
        }
        self.msg_out.put(json.dumps(msg))
        while self.orderbook.empty():
            pass
        return self.orderbook.get()
    
    def place_order(self, instrument_name, amount):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 3,
        "method" : "private/buy",
        "params" : {
            "instrument_name" : instrument_name,
            "amount" : amount
            }
        }
        self.msg_out.put(json.dumps(msg))
        while self.buy_reply.empty():
            pass
        return self.buy_reply.get()

    def get_order_state(self, order_id):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 4,
        "method" : "private/get_order_state",
        "params" : {
            "order_id" : "ETH-331562"
            }
        }
        while self.order_state.empty():
            pass
        return self.order_state.get()


class OptionsOrder:
    orders_id = []
    def __init__(self, instruments=None):
        if not instruments:
            self.instruments = Instruments()
        else:
            self.instruments = instruments
    def get_instruments(self):
        return self.instruments.get_instruments()
    
    # def place_orders(instrument_name, amount, side):
    #     client.get_orderbook()
    #     if(side == 'buy'):
    #         price = best_bid + tick_size
    #     else:
    #         price = best_ask - tick_size


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                            datefmt="%H:%M:%S")

    from UserCredentials import Client_Id, Client_Secret

    client_signature = ClientSignature(Client_Id, Client_Secret)
    client_websocket = ClientWebsocket(client_signature)
    instruments = Instruments(client_websocket, time_interval=50)

    time.sleep(1)
    # instruments = Instruments(time_interval=50, currency='ETH')

    # while True:
    #     print(instruments.get_instruments(int(time.time()*1000+10000000), 1300, 'put'))
    #     time.sleep(20)