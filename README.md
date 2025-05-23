# QueueMonitor Module

**Version:** 1.0.0  
**Author:** Garland Glessner  
**License:** GNU General Public License v3.0  

## Overview

QueueMonitor is a powerful module for the HACKtiveMQ Suite that allows you to monitor, browse, edit, and send messages to ActiveMQ queues and topics using the OpenWire protocol. This module provides a graphical interface for interacting with ActiveMQ message brokers, making it easy to inspect and modify messages in real-time.

## Features

- **Queue and Topic Browsing:** View all available queues and topics on the broker
- **Message Monitoring:** Monitor messages in real-time, with recursive monitoring option
- **Message Editing:** View, edit, and send messages to queues and topics
- **Properties Support:** Edit message properties before sending
- **JMX Integration:** Uses JMX to retrieve broker information
- **Java Integration:** Uses JPype to integrate with ActiveMQ Java client libraries

## Requirements

- Python 3.6 or later
- PySide6 (Qt for Python)
- JPype1 1.4.0 or later
- ActiveMQ Java client libraries (downloaded automatically by setup script)

## Setup

1. Make sure you have installed all required Python packages:
   ```
   pip install -r requirements.txt
   ```

2. Run the setup script to download required ActiveMQ JAR files:
   ```
   python modules/QueueMonitor_setup.py
   ```

3. Start HACKtiveMQ and select the QueueMonitor tab

## Usage

### Connecting to a Broker

1. Enter connection details (method, host, port, username, password)
2. Click "Connect"
3. The module will retrieve and display all available queues and topics

### Browsing Messages

1. Click on a queue or topic in the left panel
2. Messages will be displayed in the table on the right

### Monitoring Messages

1. Select a queue or topic
2. Click "Monitor" to start continuous monitoring
3. Check "Recursive" to also monitor for new queues and topics
4. Click "Stop" to stop monitoring

### Editing and Sending Messages

1. Select a message in the table to edit it, or click "Send New" to create a new message
2. Edit the message body in the text editor
3. Modify message properties in the properties field (format: `key1=value1,key2=value2`)
4. Click "Send Edited Message" to send the message

## Troubleshooting

### JAR Files Not Found

If you encounter errors about missing JAR files:

1. Make sure you've run the setup script: `python modules/QueueMonitor_setup.py`
2. Check that the JAR files were downloaded to the correct location (usually `./jars/`)
3. When starting the module, it will prompt you to select the JAR directory if it cannot be found automatically

### Connection Issues

If you cannot connect to the broker:

1. Verify that the ActiveMQ broker is running
2. Check your connection details (host, port, credentials)
3. Make sure the broker supports OpenWire protocol (default for ActiveMQ)

### JVM Errors

If you encounter JVM-related errors:

1. Make sure you have Java installed (JDK 8 or later recommended)
   - Download Java from: https://www.oracle.com/java/technologies/downloads/ 
   - Or install OpenJDK from: https://adoptium.net/

2. Set the JAVA_HOME environment variable:
   - On Windows: Run the `modules/set_java_home.bat` script
   - On Linux/Mac: Add `export JAVA_HOME=/path/to/your/java` to your .bashrc or .zshrc

3. Common errors and solutions:
   - "No JVM shared library file (jvm.dll) found": 
     - Make sure JAVA_HOME points to a valid JDK installation
     - For 64-bit Windows, verify you're using 64-bit Java
   
   - "UnsatisfiedLinkError":
     - Check that the Java architecture (32/64-bit) matches your Python architecture

4. Verify JPype is correctly installed: `pip install JPype1>=1.4.0`

5. Check the status box in the QueueMonitor tab for detailed error messages

## Integration with MinimalOpenWire

This module is designed to work with the same ActiveMQ brokers as the MinimalOpenWire Java client. It provides a graphical interface with similar capabilities but adds the ability to:

- Monitor queues and topics continuously
- Edit message content through a user-friendly interface
- Track changes in real-time
- Recursively monitor for new destinations

## Contributing

If you'd like to contribute to this module, please feel free to submit pull requests or report issues through the project's repository.

## License

This module is part of the HACKtiveMQ Suite and is licensed under the GNU General Public License v3.0. See the LICENSE file for details. 