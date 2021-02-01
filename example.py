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