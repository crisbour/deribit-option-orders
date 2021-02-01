from OptionOrderPlacer import *

if __name__ == "__main__":
    # Configure logging format: Time + message. Truncates messages written to console to 700 characters.
    logging.basicConfig(format="%(asctime)s: %(message).700s", level=logging.INFO,
                            datefmt="%H:%M:%S")

    from UserCredentials import Client_Id, Client_Secret

    client_signature = ClientSignature(Client_Id, Client_Secret)    # Create secure client signatures to log with
    client_websocket = ClientWebsocket(client_signature)            # Instance of websocket that handles interaction with Deribit
    instruments = Instruments(client_websocket)                     # Instance of Instruments that updates instruments every day and match the best instrument.
    option_order = OptionsOrder(client_websocket, instruments=instruments)  # Handles querring for instruments, placing orders and refreshing orders

    instrument_name,_,_ = option_order.get_instrument(round(1000*(time.time()+200)), 1319.3, 'call')
    while True:
        option_order.place_orders(instrument_name, 1, 'sell')
        time.sleep(50)
