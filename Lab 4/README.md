# Real-time Data Streaming with Spark and Python Sockets

This project demonstrates a simple simulation of real-time data streaming using Python sockets for data transmission and Apache Spark Structured Streaming for real-time processing.

## 📁 Project Structure

```
Lab 4/
├── main.py             # Entry point to run both sender and receiver in parallel
├── data_sender.py      # Socket server that simulates and sends streaming data
├── data_receiver.py    # Spark Structured Streaming receiver that consumes and processes the data
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

---

## 🔧 Components

### `data_sender.py`
- Simulates random numeric data (`x1`, `x2`, and `y = 2.5*x1 + 1.8*x2 + noise`)
- Uses a TCP socket server to stream the data line by line every 0.5 seconds

### `data_receiver.py`
- Uses Spark Structured Streaming to connect to the socket
- Parses incoming data into structured columns
- Can perform basic real-time processing (e.g., display, aggregate, etc.)

### `main.py`
- Launches both the sender and receiver in separate threads
- Ensures the program only exits when both threads have completed

---