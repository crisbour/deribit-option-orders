# Option Order Placer

Framework to interact with Deribit API and place option orders.

## Functionalities

The code contains 3 classes that the user need to interact with:

	- ClientSignature
	- ClientWebsocket
	- Instruments
	- OptionOrder

Let's take one by one and describe their role.

### ClientSignature

Initialization:

```python
ClientSignature(clientId, clientSecret, data='')
```

ClientSignature encrypts a message that will use to authenticate to Deribit. This avoid sending ClientSecret over the websocket, therefore increasing security.

### ClientWebsocket

Initialization:

```python
ClientWebsocket(client_signature, exchange_version = 'testnet')
```

ClientWebsocket sets up a websocket that will have the job to send all querries and receive all messages from Deribit. A separate thread is handling the websocket, extracting the responses and maintaining connection health. The other functions are to interact with ClientWebsocket externally from the thread mentioned:

- feed_instruments(*self*, *currency*)
- get_orderbook(*self*, *instrument_name*)
- place_order(*self*, *instrument_name*, *amount*, *side*, *price*)
- get_order_state(*self*, *order_id*)
- cancel_order(*self*, *order_id*)

### Instruments

Initialization

```python
Instruments(client_websocket, time_interval = 86400, currency='ETH')
```

Instruments querries about the available instruments through `/public/get_instruments` and saves *instrument_name*, *strike*, *type*, *expiration_timestamp*. 

Instruments contains **get_instruments** which finds the most suitable instruments for given parameters

```python
Instruments.get_instruments(expirytime, strike_price, type)
```

### OptionsOrder

Initialization

```python
OptionsOrder(client_websocket, instruments=instruments, delay=50)
OptionsOrder(client_websocket)
```

If no *Instrument*s object is passed to OptionsOrder, then and *Instruments* instance will be created within this class init. The parameter *delay* indicates in miliseconds how much time passes orders when looping them.

OptionOrder contain all functionalities requested:

- get_instruments(expirytime, strike_price, type)
- place_orders(instrument_name , amount , side)
- loop all orders

```python
OptionOrder.get_instruments(expirytime, strike_price, type)
OptionOrder.place_orders(instrument_name , amount , side)
```



## Usage Example



```python
from OptionOrderPlacer import *

if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s: %(message).700s", level=logging.INFO,
                            datefmt="%H:%M:%S")

    from UserCredentials import Client_Id, Client_Secret

    client_signature = ClientSignature(Client_Id, Client_Secret)
    client_websocket = ClientWebsocket(client_signature)
    instruments = Instruments(client_websocket)
    option_order = OptionsOrder(client_websocket, instruments=instruments)

    instrument_name,_,_ = option_order.get_instrument(round(1000*(time.time()+200)), 1319.3, 'call')
    while True:
        option_order.place_orders(instrument_name, 1, 'sell')
        time.sleep(50)
```

