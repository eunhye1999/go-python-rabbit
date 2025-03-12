# Message Sender

A simple RabbitMQ message producer application.

## Prerequisites

- Python 3.x
- RabbitMQ server running locally

## Installation

1. Install dependencies:
   ```
   pip install -r sender/requirements.txt
   ```

2. Ensure RabbitMQ is running locally on port 5672

## Running the Application

There are two ways to run the message sender:

### Option 1: From the project root

```bash
# From the project root directory
python run_sender.py
```

### Option 2: From within the sender directory

```bash
# Navigate to the sender directory first
cd sender
python bulk_send_message_direct.py
```

## Troubleshooting

If you encounter the error "nodename nor servname provided, or not known", make sure:
1. RabbitMQ server is running
2. The connection URL in sender/producer.py is correct for your environment

## Structure

- `sender/producer.py` - RabbitMQ message producer
- `sender/bulk_send_message.py` - Main module using the absolute import
- `sender/bulk_send_message_direct.py` - Alternative module using direct import
- `run_sender.py` - Wrapper script for running from the project root 