import threading
from data_sender import start_data_server
from data_receiver import start_streaming
import warnings


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    server_thread = threading.Thread(target=start_data_server)
    streaming_thread = threading.Thread(target=start_streaming)

    server_thread.start()
    streaming_thread.start()
    
    server_thread.join()
    streaming_thread.join()

