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

            
            # Process received message and store values of interest
            self.extract(response)
            logging.info('Successfully updated list of instruments')
            
            time.sleep(self.refresh_interval)

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
        
        # Lock access to object until writing the new data
        self._lock.acquire()
        self.instruments = local_instruments
        self.unique_expiry_time = local_unique_expiry_time
        # Release the lock
        self._lock.release()
        print('Unique expiration timestamps: {}\n'.format(self.unique_expiry_time))


    def get_instrument(self, expirytime, strike_price, type):
        ''' Return the instruments that match best with the given parameters.

            Parameters
            ----------
            expirytime:     int     (miliseconds)            
            strike_price:   float            
            type:           str     ('put'/'call')          

            Returns
            -------
            (instrument_name, expiration_timestamp, strike)
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


class ClientSignature:
    ''' ---- Create client signature for secure authentification ---- 
        This way you don't need to make clienSecret visible in communicating
        with the api server.'''
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



class ClientWebsocket:
    ''' Client Websocket to interact with the server, pulling and pushing json messages.
        An instance of this class creates a thread in whichan asyncio websocket is opened. 
        Asyncrhonous to it, messages are queued and dequed to get data to right member 
        or schedule new transmissions.'''

    # Configurations constants
    REFRSH_TOKEN   =   0x01     # Refresh token flgas
    AUTH_REFRESH    =   0x02    # Authentification refresh flag
    HEART_REQ       =   0x04    # Heartbeat requested flag
    HEART_SET       =   0x08    # Heartbeat set flag
    class Flags:
        ''' Flags to keep track of the connection status.'''
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
        self.exchange_version = DeribitExhchangeVersion(exchange_version).get()
        self.scope = "account:read_write trade:read_write wallet:read_write block_trade:read_write custody:read_write"
        self.msg_out = queue.Queue()
        self.instruments = queue.Queue()
        self.orderbook = queue.Queue()
        self.order_state = queue.Queue()
        self.order_reply = queue.Queue()
        self.id2queue = {1: None, 2: self.orderbook, 3: self.order_reply, 4: self.order_state, 5: self.instruments}
        self.flags = self.Flags()
        self.refersh_token = ''
        self.client_auth(client_signature)
        self.heartbeat()
        
        # Create thread for transmission and reception
        # self._lock = threading.Lock()
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
        # Concurancy for websocket, attributing incoming data and maintaining connection health
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
            while True:
                if not self.msg_out.empty():
                    msg = self.msg_out.get()
                    logging.debug('Sending: '+ msg)
                    await websocket.send(msg)
                else:
                    await asyncio.sleep(0.005)       # Backpressure to give receiving opportunity

        async def receive(websocket):
            while True:
                if websocket.open:
                    response = await websocket.recv()
                    logging.debug('Receiving: '+ response)
                    await msg_in.put(response)

        async with websockets.connect(self.exchange_version) as websocket:
            logging.debug("Socket heartbeat")
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
            if 'params' in msg.keys() and msg['params']['type'] == 'test_request':  
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
        logging.debug("Querry for get_book_summary_by_instrument placed")
        while self.orderbook.empty():
            pass
        return self.orderbook.get()
    
    def place_order(self, instrument_name, amount, side, price):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 3,
        "method" : "private/"+side,
        "params" : {
            "instrument_name" : instrument_name,
            "amount" : amount,
            "type" : "limit",
            "price": price,
            "time_in_force": "good_til_cancelled"
            }
        }
        self.msg_out.put(json.dumps(msg))
        while self.order_reply.empty():
            pass
        return self.order_reply.get()

    def get_order_state(self, order_id):
        msg = \
        {
        "jsonrpc" : "2.0",
        "id" : 4,
        "method" : "private/get_order_state",
        "params" : {
            "order_id" : order_id
            }
        }
        self.msg_out.put(json.dumps(msg))
        while self.order_state.empty():
            pass
        return self.order_state.get()
    
    def cancel_order(self, order_id):
        pass


class OptionsOrder:
    ''' Class that contains functionalities for placing option orders.'''

    def __init__(self, client_websocket, instruments = None, repeat_interval = 50):
        ''' Initialize object that handles the orders.
            Parameters
            ----------
            clien_websocket:    ClientWebsocket
            instruments:        Instruments
            repeat_interval:    int (miliseconds)
        '''

        self.client_websocket = client_websocket
        if not instruments:
            self.instruments = Instruments(client_websocket)
        else:
            self.instruments = instruments
        self.tick_size = 0.0005
        self.orders = queue.Queue()
        self.loop_orders_thread = self.loop_orders()
        

    def get_instrument(self,expiry_time, strike, type):
        return self.instruments.get_instrument(expiry_time, strike, type)
    
    def place_orders(self, instrument_name, amount, side):
        book = self.client_websocket.get_orderbook(instrument_name)
        best_bid = book['result'][0]['bid_price']
        best_ask = book['result'][0]['ask_price']
        logging.info(f'Best bid = {best_bid}, Best ask = {best_ask} \n')

        try:
            if(side == 'buy'):
                price = best_bid + self.tick_size
            else:
                price = best_ask - self.tick_size
            order_reply = self.client_websocket.place_order(instrument_name, amount, side, price)
            index = order_reply['result']['order']['order_id']
            self.orders.put(index)
            logging.info(f'{side.capitalize()} order placed for instrument {instrument_name}: ' +
                            f'(amount, price, order_id) = ({amount}, {price}, {index})')
        except:
            price_type = 'ask_price' if best_bid else ( 'best_price' if best_ask else 'best_price/ask_price')
            logging.info(f"No {price_type} available. Couldn't place {side} order for instrument {instrument_name}")

    @threaded
    def loop_orders(self):
        order_id = self.orders.get()
        order_querry = self.client_websocket.get_order_state(order_id)
        order_result = order_querry['result']
        try:
            order_state = order_result['order_state']
            if order_state == 'open':
                amount = order_result['amount']
                filled_amount = order_result['filled_amount']
                direction = order_result['direction']
                instrument = order_result['instrument_name']
                logging.info(f'Cancel order {order_id} and place_orders({instrument}, {amount-filled_amount}, {direction})')
                self.client_websocket.cancel_order(order_id)
                self.place_orders(instrument, amount - filled_amount, direction)
            else:
                logging.info(f'Order {order_id} has state \'{order_state}\'. The order has been removed from orders array.')
        except:
            logging.error(f'Order state of {order_id} is not existing or has bad format. Received json message: {order_querry}')


    

if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s: %(message).700s", level=logging.INFO,
                            datefmt="%H:%M:%S")

    from UserCredentials import Client_Id, Client_Secret

    client_signature = ClientSignature(Client_Id, Client_Secret)
    client_websocket = ClientWebsocket(client_signature)
    instruments = Instruments(client_websocket)
    option_order = OptionsOrder(client_websocket, instruments=instruments)

    instrument_name,_,_ = option_order.get_instrument(round(1000*(time.time()+200)), 1333.5, 'call')
    while True:
        option_order.place_orders(instrument_name, 1, 'buy')
        option_order.loop_orders()
        time.sleep(50)
