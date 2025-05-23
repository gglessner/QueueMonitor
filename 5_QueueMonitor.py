# QueueMonitor - part of the HACKtiveMQ Suite
# Copyright (C) 2025 Garland Glessner - gglesner@gmail.com
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from PySide6.QtWidgets import (QWidget, QPlainTextEdit, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QFrame, QGridLayout, 
                             QFileDialog, QSpacerItem, QSizePolicy, QListWidget, 
                             QListWidgetItem, QComboBox, QTableWidget, QTableWidgetItem,
                             QHeaderView, QTextEdit, QSplitter, QMessageBox, QCheckBox)
from PySide6.QtGui import QFont, QTextCursor
from PySide6.QtCore import Qt, QTimer, Signal, QObject, QThread
import os
import sys
import time
import threading
import jpype
import jpype.imports
from jpype.types import *
import socket
import csv
import subprocess
from PySide6.QtWidgets import QApplication
import random

# Import winreg only on Windows
if sys.platform == 'win32':
    import winreg
    
# Define the version number
VERSION = "1.0.1"

# Define the tab label
TAB_LABEL = f"QueueMonitor v{VERSION}"

def find_java_home():
    """Find Java home directory from environment variables or registry"""
    # First check JAVA_HOME environment variable
    java_home = os.environ.get('JAVA_HOME')
    if java_home and os.path.exists(java_home):
        return java_home
    
    # On Windows, check registry
    if sys.platform == 'win32':
        try:
            # Look for JDK
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r'SOFTWARE\JavaSoft\JDK') as key:
                current_version, _ = winreg.QueryValueEx(key, 'CurrentVersion')
                with winreg.OpenKey(key, current_version) as version_key:
                    java_home, _ = winreg.QueryValueEx(version_key, 'JavaHome')
                    if os.path.exists(java_home):
                        return java_home
        except WindowsError:
            pass
            
        try:
            # Look for JRE
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r'SOFTWARE\JavaSoft\Java Runtime Environment') as key:
                current_version, _ = winreg.QueryValueEx(key, 'CurrentVersion')
                with winreg.OpenKey(key, current_version) as version_key:
                    java_home, _ = winreg.QueryValueEx(version_key, 'JavaHome')
                    if os.path.exists(java_home):
                        return java_home
        except WindowsError:
            pass
    
    # Additional checks for macOS
    if sys.platform == 'darwin':
        # Common macOS Java home locations
        macos_java_locations = [
            '/Library/Java/JavaVirtualMachines',
            '/System/Library/Java/JavaVirtualMachines',
            os.path.expanduser('~/Library/Java/JavaVirtualMachines')
        ]
        
        for location in macos_java_locations:
            if os.path.exists(location):
                # Find the newest JDK directory
                jdk_dirs = [d for d in os.listdir(location) if d.startswith('jdk') and os.path.isdir(os.path.join(location, d))]
                if jdk_dirs:
                    # Sort by version (assuming format like jdk-11.0.1.jdk)
                    jdk_dirs.sort(reverse=True)
                    java_home = os.path.join(location, jdk_dirs[0], 'Contents/Home')
                    if os.path.exists(java_home):
                        return java_home
        
        # Try with /usr/libexec/java_home command
        try:
            java_home = subprocess.check_output(['/usr/libexec/java_home'], universal_newlines=True).strip()
            if os.path.exists(java_home):
                return java_home
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
    
    # Additional checks for Linux
    if sys.platform.startswith('linux'):
        # Common Linux Java home locations
        linux_java_locations = [
            '/usr/lib/jvm',
            '/usr/java',
            '/opt/java',
            '/usr/local/java'
        ]
        
        for location in linux_java_locations:
            if os.path.exists(location):
                # Try to find a JDK directory
                for root, dirs, _ in os.walk(location):
                    for dir_name in dirs:
                        if 'jdk' in dir_name.lower() or 'java' in dir_name.lower():
                            java_home = os.path.join(root, dir_name)
                            # Check if it looks like a Java home
                            if os.path.exists(os.path.join(java_home, 'bin', 'java')):
                                return java_home
    
    # Try using 'where java' on Windows or 'which java' on Unix
    try:
        if sys.platform == 'win32':
            java_path = subprocess.check_output(['where', 'java'], universal_newlines=True).strip().split('\n')[0]
        else:
            java_path = subprocess.check_output(['which', 'java'], universal_newlines=True).strip()
        
        # java_path is the java executable, we need its parent directory's parent
        bin_dir = os.path.dirname(java_path)
        java_home = os.path.dirname(bin_dir)
        if os.path.exists(java_home):
            return java_home
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    
    return None

def find_jvm_dll(java_home):
    """Find jvm.dll or libjvm.so based on platform and Java home"""
    if not java_home:
        return None
    
    if sys.platform == 'win32':
        # Windows
        if os.path.exists(os.path.join(java_home, 'jre')):
            # JDK layout
            search_paths = [
                os.path.join(java_home, 'jre', 'bin', 'server'),
                os.path.join(java_home, 'jre', 'bin', 'client'),
                os.path.join(java_home, 'bin', 'server'),
                os.path.join(java_home, 'bin', 'client'),
            ]
        else:
            # JRE layout
            search_paths = [
                os.path.join(java_home, 'bin', 'server'),
                os.path.join(java_home, 'bin', 'client'),
            ]
        jvm_name = 'jvm.dll'
    else:
        # Unix/Linux/Mac
        if os.path.exists(os.path.join(java_home, 'jre')):
            # JDK layout
            search_paths = [
                os.path.join(java_home, 'jre', 'lib', os.uname()[0].lower() + '_' + os.uname()[4], 'server'),
                os.path.join(java_home, 'jre', 'lib', os.uname()[0].lower() + '_' + os.uname()[4], 'client'),
            ]
        else:
            # JRE layout
            search_paths = [
                os.path.join(java_home, 'lib', os.uname()[0].lower() + '_' + os.uname()[4], 'server'),
                os.path.join(java_home, 'lib', os.uname()[0].lower() + '_' + os.uname()[4], 'client'),
            ]
        jvm_name = 'libjvm.so'
        if sys.platform == 'darwin':
            jvm_name = 'libjvm.dylib'
    
    for path in search_paths:
        jvm_path = os.path.join(path, jvm_name)
        if os.path.exists(jvm_path):
            return jvm_path
    
    return None

class JVMThread(QThread):
    """Thread for running the JVM and interacting with ActiveMQ"""
    error_signal = Signal(str)
    log_signal = Signal(str)  # Standard log messages
    update_queues_signal = Signal(list)
    update_topics_signal = Signal(list)
    update_messages_signal = Signal(list)
    
    def __init__(self):
        super().__init__()
        self.running = False
        self.connection = None
        self.session = None
        self.monitoring_threads = {}  # Dictionary to store monitoring threads by destination
        self.monitor_running = False
        self.recursive_monitor = False
        self.current_destination = None
        self.connection_params = {}
        self.message_cache = {}  # Cache for messages from different destinations
        self.message_cache_lock = threading.Lock()  # Lock for thread-safe access to message cache
        # Store JMS package info at class level
        self.using_jakarta = True
        self.jms_package = None
        self.jakarta = None
        self.javax = None
        # For active topic monitoring
        self.topic_consumers = {}  # Dictionary to store active topic consumers
        self.topic_listeners = {}  # Dictionary to store message listeners for topics
        # For unified monitoring
        self.monitored_queues = []
        self.monitored_topics = []
        self.monitor_thread = None
    
    def start_jvm(self, jars_path):
        """Start the JVM with the required JAR files"""
        try:
            if not jpype.isJVMStarted():
                jar_files = []
                for file in os.listdir(jars_path):
                    if file.endswith('.jar'):
                        jar_files.append(os.path.join(jars_path, file))
                
                classpath = os.pathsep.join(jar_files)
                
                # Try to find JVM location
                java_home = find_java_home()
                jvm_path = None
                
                if java_home:
                    self.log_signal.emit(f"Found Java home: {java_home}")
                    jvm_path = find_jvm_dll(java_home)
                    if jvm_path:
                        self.log_signal.emit(f"Found JVM at: {jvm_path}")
                
                # Basic JVM arguments
                jvm_args = [f"-Djava.class.path={classpath}"]
                
                # Add modern Java arguments to avoid warnings about restricted calls
                jvm_args.append("--enable-native-access=ALL-UNNAMED")
                
                if jvm_path:
                    jpype.startJVM(jvm_path, *jvm_args, convertStrings=False)
                else:
                    # Fall back to default JVM path
                    jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args, convertStrings=False)
                
                self.log_signal.emit(f"JVM started with classpath: {classpath}")
            return True
        except Exception as e:
            self.error_signal.emit(f"Error starting JVM: {str(e)}")
            self.error_signal.emit("Make sure Java is installed and JAVA_HOME is set correctly.")
            return False
    
    def connect_to_broker(self, method, host, port, username, password):
        """Connect to the ActiveMQ broker"""
        try:
            self.connection_params = {
                'method': method,
                'host': host,
                'port': port,
                'username': username,
                'password': password
            }
            
            # Import required Java classes - handle both javax (older) and jakarta (newer) JMS packages
            try:
                # Try newer Jakarta EE packages first
                self.jakarta = jpype.JPackage("jakarta")
                self.jms_package = self.jakarta.jms
                self.using_jakarta = True
                # Jakarta JMS API detection logged only when actually used
            except Exception:
                try:
                    # Fall back to older Java EE packages
                    self.javax = jpype.JPackage("javax")
                    self.jms_package = self.javax.jms
                    self.using_jakarta = False
                    # JavaX JMS API detection logged only when actually used
                except Exception as e:
                    self.error_signal.emit(f"Could not load JMS packages: {str(e)}")
                    self.error_signal.emit("Make sure ActiveMQ JARs are in the correct location")
                    return False
            
            # Create connection factory
            connection_url = f"{method}://{host}:{port}"
            self.log_signal.emit(f"Connecting to ActiveMQ at {connection_url}")
            
            # Check if ActiveMQ is running first using a simple socket check
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)  # 2 second timeout
            try:
                sock.connect((host, int(port)))
                sock.close()
            except socket.error:
                self.error_signal.emit(f"Could not connect to {host}:{port}")
                self.error_signal.emit("Please make sure ActiveMQ is running and accessible")
                self.error_signal.emit("You can start ActiveMQ using the LoadActiveMQ tab")
                self.error_signal.emit("Wrong host/port - verify connection settings")
                return False
            
            # Import ActiveMQ connection factory
            ActiveMQConnectionFactory = jpype.JClass("org.apache.activemq.ActiveMQConnectionFactory")
            factory = ActiveMQConnectionFactory(connection_url)
            
            if username and password:
                factory.setUserName(username)
                factory.setPassword(password)
            
            # Create connection and session
            self.connection = factory.createConnection()
            self.connection.start()
            
            # Use the correct session constant based on JMS package version
            try:
                # Try to get the AUTO_ACKNOWLEDGE constant from the package
                AUTO_ACKNOWLEDGE = self.jms_package.Session.AUTO_ACKNOWLEDGE
            except:
                # Hardcode the value if we can't get it (1 is the standard value)
                AUTO_ACKNOWLEDGE = 1
                # Removing debug message about hardcoded AUTO_ACKNOWLEDGE
            
            self.session = self.connection.createSession(False, AUTO_ACKNOWLEDGE)
            
            self.log_signal.emit(f"Connected to broker at {connection_url}")
            return True
        except Exception as e:
            self.error_signal.emit(f"Error connecting to broker: {str(e)}")
            import traceback
            self.error_signal.emit(traceback.format_exc())
            return False
    
    def disconnect(self):
        """Disconnect from the broker and stop the JVM"""
        try:
            if self.session:
                self.session.close()
                self.session = None
            
            if self.connection:
                self.connection.close()
                self.connection = None
            
            self.log_signal.emit("Disconnected from broker")
            return True
        except Exception as e:
            self.error_signal.emit(f"Error disconnecting: {str(e)}")
            return False
    
    def get_destinations(self):
        """Get all queues and topics from the broker using the DestinationSource from the ActiveMQConnection"""
        try:
            if not self.connection or not self.session:
                self.error_signal.emit("Not connected to broker")
                return [], []
            
            queues = []
            topics = []
            
            # Use ActiveMQConnection's getDestinationSource method like in MinimalOpenWire2.py
            try:
                # Check if connection is ActiveMQConnection
                if hasattr(self.connection, 'getDestinationSource'):
                    # Get the destination source
                    destination_source = self.connection.getDestinationSource()
                    
                    # Get queues
                    all_queues = destination_source.getQueues()
                    if all_queues:
                        queue_iterator = all_queues.iterator()
                        while queue_iterator.hasNext():
                            queue = queue_iterator.next()
                            if hasattr(queue, 'getQueueName'):
                                queue_name = str(queue.getQueueName())
                                if queue_name not in queues:
                                    queues.append(queue_name)
                                    self.log_signal.emit(f"Found queue: {queue_name}")
                    
                    # Get topics
                    all_topics = destination_source.getTopics()
                    if all_topics:
                        topic_iterator = all_topics.iterator()
                        while topic_iterator.hasNext():
                            topic = topic_iterator.next()
                            topic_name = str(topic)
                            if topic_name not in topics and not topic_name.startswith("ActiveMQ.Advisory"):
                                topics.append(topic_name)
                                self.log_signal.emit(f"Found topic: {topic_name}")
                    
                    # Log the total found
                    if queues or topics:
                        self.log_signal.emit(f"Found {len(queues)} queues and {len(topics)} topics via DestinationSource")
            except Exception as e:
                self.log_signal.emit(f"DestinationSource error: {str(e)}::non-critical")
            
            # If no destinations found, try to use advisory topics as a fallback
            if not queues and not topics:
                try:
                    # Create temp consumer for advisory topics
                    advisory_topic = self.session.createTopic("ActiveMQ.Advisory.Queue")
                    temp_consumer = self.session.createConsumer(advisory_topic)
                    
                    # Try to receive advisory messages with short timeout
                    for _ in range(3):
                        msg = temp_consumer.receive(100)
                        if msg:
                            try:
                                if hasattr(msg, "getStringProperty"):
                                    queue_name = msg.getStringProperty("destinationName")
                                    if queue_name and queue_name not in queues:
                                        queues.append(queue_name)
                                        self.log_signal.emit(f"Discovered queue: {queue_name}")
                            except:
                                pass
                    
                    temp_consumer.close()
                    
                    # Do the same for topics
                    advisory_topic = self.session.createTopic("ActiveMQ.Advisory.Topic")
                    temp_consumer = self.session.createConsumer(advisory_topic)
                    
                    for _ in range(3):
                        msg = temp_consumer.receive(100)
                        if msg:
                            try:
                                if hasattr(msg, "getStringProperty"):
                                    topic_name = msg.getStringProperty("destinationName")
                                    if topic_name and topic_name not in topics and not topic_name.startswith("ActiveMQ.Advisory"):
                                        topics.append(topic_name)
                                        self.log_signal.emit(f"Discovered topic: {topic_name}")
                            except:
                                pass
                    
                    temp_consumer.close()
                except Exception as e:
                    self.log_signal.emit(f"Advisory discovery error: {str(e)}::non-critical")
            
            # If we found destinations, update cache
            if queues or topics:
                self.log_signal.emit(f"Total: {len(queues)} queues and {len(topics)} topics")
                self._cached_queues = queues
                self._cached_topics = topics
            else:
                # Use cached destinations if available
                if hasattr(self, '_cached_queues') and hasattr(self, '_cached_topics'):
                    queues = self._cached_queues
                    topics = self._cached_topics
                    self.log_signal.emit("Using cached destination list")
                else:
                    # No destinations found and no cache - return empty lists
                    self.log_signal.emit("No destinations found")
                    # Store empty lists for next time
                    self._cached_queues = []
                    self._cached_topics = []
            
            self.update_queues_signal.emit(queues)
            self.update_topics_signal.emit(topics)
            return queues, topics
            
        except Exception as e:
            self.error_signal.emit(f"Error getting destinations: {str(e)}")
            # Return empty lists on error - no defaults
            self.update_queues_signal.emit([])
            self.update_topics_signal.emit([])
            return [], []
    
    def browse_queue(self, queue_name, update_ui=True, auto_refresh=False, force_new_browser=False):
        """Browse messages in a queue"""
        try:
            if not self.connection or not self.session:
                self.error_signal.emit("Not connected to broker")
                return []
            
            # Create queue and browser
            queue = self.session.createQueue(queue_name)
            
            # When force_new_browser is True, explicitly close and recreate the session
            # This ensures we get a fresh view of the queue contents
            if force_new_browser:
                try:
                    # Create a new session for a fresh browser view
                    temp_session = self.connection.createSession(False, 1)  # AUTO_ACKNOWLEDGE
                    browser = temp_session.createBrowser(queue)
                except Exception:
                    # Fall back to regular session if creating a new one fails
                    browser = self.session.createBrowser(queue)
            else:
                browser = self.session.createBrowser(queue)
            
            # Get messages
            messages = []
            message_enum = browser.getEnumeration()
            
            # Process messages
            message_count = 0
            while message_enum.hasMoreElements():
                try:
                    message = message_enum.nextElement()
                    
                    # Skip null messages
                    if message is None:
                        continue
                    
                    # Process valid message
                    try:
                        message_id = str(message.getJMSMessageID())
                        message_count += 1
                        
                        # Get message properties
                        properties = {}
                        property_enum = message.getPropertyNames()
                        while property_enum.hasMoreElements():
                            prop_name = str(property_enum.nextElement())
                            properties[prop_name] = str(message.getStringProperty(prop_name))
                        
                        # Simple message body extraction
                        body = "Unknown data type"
                        message_type = "unknown"
                        
                        try:
                            message_class = message.getClass().getName()
                            
                            if "TextMessage" in message_class:
                                message_type = "text"
                                body = str(message.getText())
                            elif hasattr(message, "toString"):
                                body = str(message.toString())
                                message_type = "string"
                        except Exception:
                            body = str(message)
                        
                        messages.append({
                            'id': message_id,
                            'body': body,
                            'properties': properties,
                            'destination': str(queue_name),
                            'type': 'queue',
                            'message_type': message_type,
                            'timestamp': int(message.getJMSTimestamp())
                        })
                    except Exception:
                        continue
                except Exception:
                    break
            
            # Close browser
            try:
                browser.close()
                # Also close the temporary session if we created one
                if force_new_browser and 'temp_session' in locals():
                    try:
                        temp_session.close()
                    except Exception:
                        pass
            except Exception:
                pass
            
            # Log message count for new messages or manual operations
            if message_count > 0 and not auto_refresh:
                self.log_signal.emit(f">>> UPDATE: Queue {queue_name} found {message_count} messages")
            
            # Update UI if requested
            if update_ui:
                self.update_messages_signal.emit(messages)
                
            return messages
        except Exception as e:
            self.error_signal.emit(f"Error browsing queue {queue_name}: {str(e)}::non-critical")
            return []
    
    def subscribe_to_topic(self, topic_name, update_ui=True, auto_refresh=False):
        """Subscribe to a topic and get messages
        
        Args:
            topic_name: Name of the topic to subscribe to
            update_ui: Whether to update the UI with the messages
            auto_refresh: Set to True if this is an automatic refresh after sending a message
        """
        try:
            if not self.connection or not self.session:
                self.error_signal.emit("Not connected to broker")
                return []
            
            # Check if this topic already has an active consumer with a MessageListener
            topic_key = f"topic:{topic_name}"
            if topic_key in self.topic_consumers:
                # If we already have a listener, just return any cached messages
                with self.message_cache_lock:
                    if topic_key in self.message_cache:
                        messages = self.message_cache[topic_key]
                        # Only emit signal to update UI if requested
                        if update_ui:
                            self.update_messages_signal.emit(messages)
                        return messages
                    else:
                        # No cached messages yet, just return empty list
                        if update_ui:
                            self.update_messages_signal.emit([])
                        return []
            
            # Create topic and consumer
            topic = self.session.createTopic(topic_name)
            consumer = self.session.createConsumer(topic)
            
            # Get messages (non-blocking with timeout)
            messages = []
            message = consumer.receive(100)  # 100ms timeout
            
            # If no messages available, log it and return empty list
            if message is None:
                self.log_signal.emit(f"No messages available on topic {topic_name}")
                consumer.close()
                
                # Only emit signal to update UI if requested
                if update_ui:
                    self.update_messages_signal.emit(messages)
                    
                return messages
            
            # Process messages if they exist
            while message is not None:
                try:
                    message_id = str(message.getJMSMessageID())
                    
                    # Get message properties
                    properties = {}
                    property_enum = message.getPropertyNames()
                    while property_enum.hasMoreElements():
                        prop_name = str(property_enum.nextElement())
                        properties[prop_name] = str(message.getStringProperty(prop_name))
                    
                    # Enhanced message body extraction
                    body = "Unknown data type"
                    message_type = "unknown"
                    
                    # Try multiple approaches to get the message body
                    try:
                        # Approach 1: Direct type checking and conversion
                        if self.using_jakarta:
                            jms_package = self.jakarta.jms
                        else:
                            jms_package = self.javax.jms
                        
                        message_class = message.getClass().getName()
                        
                        # Check message type more directly
                        if "TextMessage" in message_class:
                            message_type = "text"
                            body = str(message.getText())
                        elif "BytesMessage" in message_class:
                            message_type = "bytes"
                            if hasattr(message, "getText"):
                                body = str(message.getText())
                            else:
                                body = "Binary data (not readable as text)"
                        elif "MapMessage" in message_class:
                            message_type = "map"
                            # Try to get map content as a string representation
                            map_content = {}
                            map_names = message.getMapNames()
                            while map_names.hasMoreElements():
                                key = str(map_names.nextElement())
                                try:
                                    value = str(message.getObject(key))
                                    map_content[key] = value
                                except:
                                    map_content[key] = "[Complex object]"
                            body = str(map_content)
                        elif "ObjectMessage" in message_class:
                            message_type = "object"
                            try:
                                java_obj = message.getObject()
                                body = f"Java object: {java_obj.getClass().getName()}"
                            except:
                                body = "Java object (not convertible to string)"
                        elif "StreamMessage" in message_class:
                            message_type = "stream"
                            body = "Stream data (not displayed in text form)"
                        else:
                            # Fallback approach
                            if hasattr(message, "getText"):
                                body = str(message.getText())
                                message_type = "text (fallback)"
                            elif hasattr(message, "toString"):
                                body = str(message.toString())
                                message_type = "string (toString)"
                    except Exception as e:
                        self.error_signal.emit(f"Error extracting message body: {str(e)}::non-critical")
                        # One final attempt - try toString() if it exists
                        try:
                            body = str(message)
                            message_type = "string (str conversion)"
                        except:
                            pass
                    
                    messages.append({
                        'id': message_id,
                        'body': body,
                        'properties': properties,
                        'destination': str(topic_name),
                        'type': 'topic',
                        'message_type': message_type,
                        'timestamp': int(message.getJMSTimestamp())
                    })
                except Exception as e:
                    self.error_signal.emit(f"Error processing message from topic {topic_name}: {str(e)}::non-critical")
                    continue
                
                message = consumer.receive(100)  # Get next message with timeout
            
            consumer.close()
            
            # Only emit signal to update UI if requested
            if update_ui:
                self.update_messages_signal.emit(messages)
                
            return messages
        except Exception as e:
            self.error_signal.emit(f"Error subscribing to topic: {str(e)}::non-critical")
            import traceback
            self.log_signal.emit(traceback.format_exc())
            return []
    
    def send_message(self, destination_name, destination_type, message_body, properties=None):
        """Send a message to a queue or topic"""
        try:
            if not self.connection or not self.session:
                self.error_signal.emit("Not connected to broker")
                return False
            
            # Create destination
            if destination_type == 'queue':
                destination = self.session.createQueue(destination_name)
            else:  # topic
                destination = self.session.createTopic(destination_name)
            
            # Create producer and message
            producer = self.session.createProducer(destination)
            text_message = self.session.createTextMessage(message_body)
            
            # Set properties
            if properties:
                for key, value in properties.items():
                    text_message.setStringProperty(key, value)
            
            # Send message
            producer.send(text_message)
            producer.close()
            
            # Single log message about the sent message
            self.log_signal.emit(f"Message sent to {destination_type} {destination_name}")
            return True
        except Exception as e:
            self.error_signal.emit(f"Error sending message: {str(e)}")
            return False
    
    def start_monitoring(self, destination_name, destination_type, recursive=True):
        """Start monitoring a destination by adding it to the monitor thread
        
        This just adds the destination to the appropriate list and starts monitor_all.
        """
        try:
            # Create destination lists if they don't exist
            if not hasattr(self, 'monitored_queues'):
                self.monitored_queues = []
            if not hasattr(self, 'monitored_topics'):
                self.monitored_topics = []
            
            # Add to appropriate list
            if destination_type == 'queue' and destination_name not in self.monitored_queues:
                self.monitored_queues.append(destination_name)
            elif destination_type == 'topic' and destination_name not in self.monitored_topics:
                self.monitored_topics.append(destination_name)
            
            # Start monitoring with current lists
            self.monitor_all(self.monitored_queues, self.monitored_topics)
            
        except Exception as e:
            self.error_signal.emit(f"Error starting monitoring: {str(e)}")
    
    def monitor_all(self, queues, topics, recursive=True):
        """Start monitoring all queues and topics - ultra simple version"""
        # First stop any existing monitoring
        self.emergency_stop_monitoring()
        
        # No destinations to monitor
        if not queues and not topics:
            self.log_signal.emit("No destinations to monitor")
            return
        
        # Store lists
        self.monitored_queues = list(queues)
        self.monitored_topics = list(topics)
        
        # Initialize message cache
        with self.message_cache_lock:
            self.message_cache.clear()
        
        # Set up simple queue monitoring
        def queue_monitor_thread():
            try:
                # Initial reading of queue contents
                all_messages = []
                for queue in self.monitored_queues:
                    try:
                        messages = self.browse_queue(queue, update_ui=False, auto_refresh=True, force_new_browser=True)
                        dest_key = f"queue:{queue}"
                        with self.message_cache_lock:
                            self.message_cache[dest_key] = messages
                            all_messages.extend(messages)
                    except Exception:
                        pass
                
                # Update UI with initial messages
                if all_messages:
                    self.update_messages_signal.emit(all_messages)
                    
                # Track last check time and message IDs for each queue
                last_checks = {}
                last_message_ids = {}
                for queue in self.monitored_queues:
                    last_checks[queue] = time.time()
                    last_message_ids[queue] = set()
                    # Initialize with current message IDs
                    dest_key = f"queue:{queue}"
                    if dest_key in self.message_cache:
                        last_message_ids[queue] = {msg['id'] for msg in self.message_cache[dest_key]}
                
                # Main monitoring loop - keep as simple as possible
                while self.monitor_running:
                    time.sleep(0.5)  # Check every half second
                    
                    # Check each queue
                    current_time = time.time()
                    new_messages_detected = False  # Flag to track if any new messages were found
                    
                    for queue in self.monitored_queues:
                        # Check every queue more frequently when in monitor_all mode
                        if current_time - last_checks.get(queue, 0) < 1.0:  # Reduced from 2.0 to 1.0 seconds
                            continue
                            
                        try:
                            # Always use force_new_browser=True for reliable detection of new messages
                            messages = self.browse_queue(queue, update_ui=False, auto_refresh=True, force_new_browser=True)
                            last_checks[queue] = current_time
                            
                            # Check for new messages by comparing IDs
                            if messages:
                                current_ids = {msg['id'] for msg in messages}
                                
                                # Check if we have any new messages
                                if queue in last_message_ids:
                                    new_ids = current_ids - last_message_ids[queue]
                                    if new_ids:
                                        self.log_signal.emit(f">>> Found {len(new_ids)} new messages in queue {queue}")
                                        new_messages_detected = True
                                        
                                        # Update cache with the queue's messages
                                        with self.message_cache_lock:
                                            dest_key = f"queue:{queue}"
                                            self.message_cache[dest_key] = messages
                                
                                # Always update the tracked IDs
                                last_message_ids[queue] = current_ids
                            
                        except Exception as e:
                            # Log errors during queue monitoring
                            self.error_signal.emit(f"Error monitoring queue {queue}: {str(e)}::non-critical")
                    
                    # If any new messages were detected in any queue, update the UI with all messages
                    if new_messages_detected:
                        with self.message_cache_lock:
                            # Update UI with all messages from all destinations
                            all_messages = []
                            for dest, msgs in self.message_cache.items():
                                all_messages.extend(msgs)
                            
                            if all_messages:
                                self.update_messages_signal.emit(all_messages)
                            
            except Exception as e:
                self.error_signal.emit(f"Error in queue monitor: {str(e)}")
        
        # Create topic listeners one by one - do this before starting queue thread
        for topic in topics:
            try:
                # Use our create_topic_message_listener method
                consumer, _ = self.create_topic_message_listener(topic)
                if not consumer:
                    self.error_signal.emit(f"Failed to create listener for topic {topic}")
            except Exception as e:
                self.error_signal.emit(f"Error setting up topic {topic}: {str(e)}")
        
        # Set flag and start queue monitor thread AFTER setting up topics
        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=queue_monitor_thread)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.log_signal.emit(f"Monitoring started for {len(queues)} queues and {len(topics)} topics")
    
    def emergency_stop_monitoring(self):
        """Emergency stop all monitoring with minimal thread interaction"""
        try:
            # Set flag first
            self.monitor_running = False
            
            # Safety delay for thread to see flag
            time.sleep(0.1)
            
            # Just close all topic consumers and don't worry about the threads
            try:
                for key in list(self.topic_consumers.keys()):
                    try:
                        consumer = self.topic_consumers[key]
                        consumer.close()
                        self.log_signal.emit(f"Closed consumer for {key}")
                    except Exception:
                        pass
            except Exception:
                pass
            
            # Clear all collections
            self.topic_consumers.clear()
            self.topic_listeners.clear()
            self.monitored_queues = []
            self.monitored_topics = []
            
            with self.message_cache_lock:
                self.message_cache.clear()
            
            self.log_signal.emit("All monitoring stopped")
        except Exception as e:
            self.error_signal.emit(f"Error in emergency stop: {str(e)}")
    
    def _on_topic_message(self, message, topic_name):
        """Handle a message received on a topic subscription"""
        try:
            # Skip null messages
            if message is None:
                return
            
            # Get message ID and log it
            message_id = str(message.getJMSMessageID())
            self.log_signal.emit(f">>> NEW MESSAGE RECEIVED on topic {topic_name}: {message_id}")
            
            # Get message body - simple approach
            body = ""
            try:
                if hasattr(message, "getText"):
                    body = str(message.getText())
                else:
                    body = str(message)
            except Exception:
                body = "[Could not extract message body]"
            
            # Log the message body
            self.log_signal.emit(f"Message body: {body[:100]}{' (truncated)' if len(body) > 100 else ''}")
            
            # Create a simple message object
            message_info = {
                'id': message_id,
                'body': body,
                'properties': {},
                'destination': topic_name,
                'type': 'topic',
                'message_type': 'text',
                'timestamp': int(message.getJMSTimestamp())
            }
            
            # Update message cache and UI
            dest_key = f"topic:{topic_name}"
            all_messages = []
            
            # Add to cache
            with self.message_cache_lock:
                if dest_key not in self.message_cache:
                    self.message_cache[dest_key] = []
                
                self.message_cache[dest_key].append(message_info)
                
                # Get all messages for UI update
                for msgs in self.message_cache.values():
                    all_messages.extend(msgs)
            
            # Update UI from outside the lock
            if all_messages:
                self.update_messages_signal.emit(all_messages)
            
        except Exception as e:
            self.error_signal.emit(f"Error handling topic message: {str(e)}")

    def create_topic_message_listener(self, topic_name):
        """Create a message listener for a topic - simplified version
        
        Returns a tuple of (consumer, None) for compatibility with existing code
        """
        try:
            if not self.connection or not self.session:
                self.error_signal.emit("Not connected to broker")
                return None, None
            
            # Create a consumer for this topic
            topic_dest = self.session.createTopic(topic_name)
            consumer = self.session.createConsumer(topic_dest)
            
            # Create a message listener
            class SimpleTopicListener:
                def __init__(self, outer_self, topic_name):
                    self.outer = outer_self
                    self.topic = topic_name
                
                def onMessage(self, message):
                    try:
                        self.outer._on_topic_message(message, self.topic)
                    except Exception as e:
                        self.outer.error_signal.emit(f"Topic listener error: {str(e)}")
            
            # Create and register the listener
            if self.using_jakarta:
                MessageListener = jpype.JProxy("jakarta.jms.MessageListener", dict={
                    "onMessage": SimpleTopicListener(self, topic_name).onMessage
                })
            else:
                MessageListener = jpype.JProxy("javax.jms.MessageListener", dict={
                    "onMessage": SimpleTopicListener(self, topic_name).onMessage
                })
            
            # Set the listener and store the consumer
            consumer.setMessageListener(MessageListener)
            
            # Store the consumer for cleanup
            dest_key = f"topic:{topic_name}"
            self.topic_consumers[dest_key] = consumer
            
            self.log_signal.emit(f"Created active subscription for topic {topic_name}")
            return consumer, None
            
        except Exception as e:
            self.error_signal.emit(f"Error creating topic listener: {str(e)}")
            return None, None

class Ui_TabContent:
    def setupUi(self, widget):
        """Set up the UI components for the QueueMonitor tab."""
        widget.setObjectName("TabContent")

        # Main vertical layout
        self.verticalLayout = QVBoxLayout(widget)
        self.verticalLayout.setSpacing(5)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)

        # Header frame with title and connection details - make as compact as possible
        self.headerFrame = QFrame(widget)
        self.headerFrame.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.headerLayout = QHBoxLayout(self.headerFrame)
        self.headerLayout.setContentsMargins(5, 5, 5, 5)
        self.headerLayout.setSpacing(5)

        # Title on the left side only - use system font for better compatibility
        self.titleLabel = QLabel(self.headerFrame)
        # Use system font for better Mac compatibility
        font = QFont()
        font.setFamily("Arial")  # More universally available font
        font.setPointSize(48)    # Very large font size
        font.setBold(True)
        font.setWeight(QFont.Bold)
        self.titleLabel.setFont(font)
        # Set explicit text here for debugging
        self.titleLabel.setText(f"QueueMonitor v{VERSION}")
        self.headerLayout.addWidget(self.titleLabel)

        # Add a spacer to push everything else to the right
        self.headerLayout.addItem(QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum))
        
        # Status label - now on the right side
        self.statusLabel = QLabel("", self.headerFrame)
        self.statusLabel.setStyleSheet("color: green; font-weight: bold; font-size: 18px;")
        self.headerLayout.addWidget(self.statusLabel)

        # Create input widgets directly in the header layout to eliminate spacing issues
        # Larger font for all labels and input fields
        labelFont = QFont()
        labelFont.setFamily("Arial")  # Better Mac compatibility
        labelFont.setPointSize(18)
        inputFont = QFont()
        inputFont.setFamily("Arial")  # Better Mac compatibility
        inputFont.setPointSize(18)

        # Method
        methodLabel = QLabel("Method:", self.headerFrame)
        methodLabel.setFont(labelFont)
        self.headerLayout.addWidget(methodLabel)
        
        self.methodCombo = QComboBox(self.headerFrame)
        self.methodCombo.addItems(["tcp", "ssl", "nio", "auto"])
        self.methodCombo.setFixedWidth(100)
        self.methodCombo.setFont(inputFont)
        self.headerLayout.addWidget(self.methodCombo)

        # Host
        hostLabel = QLabel("Host:", self.headerFrame)
        hostLabel.setFont(labelFont)
        self.headerLayout.addWidget(hostLabel)
        
        self.hostLine = QLineEdit(self.headerFrame)
        self.hostLine.setText("localhost")
        self.hostLine.setFixedWidth(150)
        self.hostLine.setFont(inputFont)
        self.headerLayout.addWidget(self.hostLine)

        # Port
        portLabel = QLabel("Port:", self.headerFrame)
        portLabel.setFont(labelFont)
        self.headerLayout.addWidget(portLabel)
        
        self.portLine = QLineEdit(self.headerFrame)
        self.portLine.setText("61616")
        self.portLine.setFixedWidth(100)
        self.portLine.setFont(inputFont)
        self.headerLayout.addWidget(self.portLine)

        # Username
        userLabel = QLabel("User:", self.headerFrame)
        userLabel.setFont(labelFont)
        self.headerLayout.addWidget(userLabel)
        
        self.usernameLine = QLineEdit(self.headerFrame)
        self.usernameLine.setText("admin")
        self.usernameLine.setFixedWidth(100)
        self.usernameLine.setFont(inputFont)
        self.headerLayout.addWidget(self.usernameLine)

        # Password
        passLabel = QLabel("Pass:", self.headerFrame)
        passLabel.setFont(labelFont)
        self.headerLayout.addWidget(passLabel)
        
        self.passwordLine = QLineEdit(self.headerFrame)
        self.passwordLine.setText("admin")
        self.passwordLine.setEchoMode(QLineEdit.Password)
        self.passwordLine.setFixedWidth(100)
        self.passwordLine.setFont(inputFont)
        self.headerLayout.addWidget(self.passwordLine)

        # Connect button
        self.connectButton = QPushButton("Connect", self.headerFrame)
        self.connectButton.setFixedWidth(120)
        self.connectButton.setFont(labelFont)
        self.headerLayout.addWidget(self.connectButton)

        self.verticalLayout.addWidget(self.headerFrame)

        # Main splitter to hold the three main sections
        self.mainSplitter = QSplitter(Qt.Vertical)
        
        # Section 1: Content splitter (holds left panel and right panel)
        self.contentSplitter = QSplitter(Qt.Horizontal)
        
        # Left panel: Queues and Topics
        self.leftPanel = QWidget()
        self.leftLayout = QVBoxLayout(self.leftPanel)
        self.leftLayout.setContentsMargins(5, 5, 5, 5)

        # Queues list with larger font
        self.queuesLabel = QLabel("Queues:")
        self.queuesLabel.setFont(labelFont)
        self.leftLayout.addWidget(self.queuesLabel)
        
        self.queuesList = QListWidget()
        self.queuesList.setFont(inputFont)
        self.leftLayout.addWidget(self.queuesList)

        # Topics list with larger font
        self.topicsLabel = QLabel("Topics:")
        self.topicsLabel.setFont(labelFont)
        self.leftLayout.addWidget(self.topicsLabel)
        
        self.topicsList = QListWidget()
        self.topicsList.setFont(inputFont)
        self.leftLayout.addWidget(self.topicsList)

        # Refresh button with larger font
        self.refreshButton = QPushButton("Refresh Destinations")
        self.refreshButton.setFont(labelFont)
        self.leftLayout.addWidget(self.refreshButton)

        self.contentSplitter.addWidget(self.leftPanel)

        # Right panel: Messages and actions
        self.rightPanel = QWidget()
        self.rightLayout = QVBoxLayout(self.rightPanel)
        self.rightLayout.setContentsMargins(5, 5, 5, 5)

        # Messages list with larger font
        self.messagesLabel = QLabel("Messages:")
        self.messagesLabel.setFont(labelFont)
        self.rightLayout.addWidget(self.messagesLabel)
        
        self.messagesTable = QTableWidget()
        self.messagesTable.setFont(inputFont)
        self.messagesTable.setColumnCount(5)
        self.messagesTable.setHorizontalHeaderLabels(["ID", "Destination", "Type", "Timestamp", "Body"])
        self.messagesTable.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.rightLayout.addWidget(self.messagesTable)

        # Message actions frame
        self.actionsFrame = QFrame()
        self.actionsLayout = QHBoxLayout(self.actionsFrame)
        self.actionsLayout.setContentsMargins(0, 0, 0, 0)

        # Action buttons with larger font
        self.monitorButton = QPushButton("Monitor")
        self.monitorButton.setFont(labelFont)
        self.actionsLayout.addWidget(self.monitorButton)
        
        self.monitorAllButton = QPushButton("Monitor All")
        self.monitorAllButton.setFont(labelFont)
        self.actionsLayout.addWidget(self.monitorAllButton)
        
        self.stopButton = QPushButton("Stop")
        self.stopButton.setFont(labelFont)
        self.actionsLayout.addWidget(self.stopButton)
        
        self.sendButton = QPushButton("Send New")
        self.sendButton.setFont(labelFont)
        self.actionsLayout.addWidget(self.sendButton)
        
        self.exportCsvButton = QPushButton("Export CSV")
        self.exportCsvButton.setFont(labelFont)
        self.actionsLayout.addWidget(self.exportCsvButton)

        self.rightLayout.addWidget(self.actionsFrame)

        # Message editor with larger font
        self.editorLabel = QLabel("Message Editor:")
        self.editorLabel.setFont(labelFont)
        self.rightLayout.addWidget(self.editorLabel)
        
        self.messageEditor = QTextEdit()
        self.messageEditor.setFont(inputFont)
        self.rightLayout.addWidget(self.messageEditor)

        # Properties editor with larger font
        self.propertiesFrame = QFrame()
        self.propertiesLayout = QHBoxLayout(self.propertiesFrame)
        self.propertiesLayout.setContentsMargins(0, 0, 0, 0)
        
        self.propertiesLabel = QLabel("Properties:")
        self.propertiesLabel.setFont(labelFont)
        self.propertiesLayout.addWidget(self.propertiesLabel)
        
        self.propertiesLine = QLineEdit()
        self.propertiesLine.setPlaceholderText("property1=value1,property2=value2")
        self.propertiesLine.setFont(inputFont)
        self.propertiesLayout.addWidget(self.propertiesLine)

        self.rightLayout.addWidget(self.propertiesFrame)

        # Send edited message button with larger font
        self.sendEditedButton = QPushButton("Send Edited Message")
        self.sendEditedButton.setFont(labelFont)
        self.rightLayout.addWidget(self.sendEditedButton)

        self.contentSplitter.addWidget(self.rightPanel)
        
        # Set initial content splitter sizes (30% left, 70% right)
        self.contentSplitter.setSizes([300, 700])
        
        # Add the content splitter to the main splitter
        self.mainSplitter.addWidget(self.contentSplitter)

        # Section 2: Status text box
        self.statusTextBox = QPlainTextEdit()
        self.statusTextBox.setReadOnly(True)
        status_font = QFont("Courier New", 18)  # Increased font size to 18
        status_font.setFixedPitch(True)
        self.statusTextBox.setFont(status_font)
        self.mainSplitter.addWidget(self.statusTextBox)
        
        # Add the main splitter to the layout
        self.verticalLayout.addWidget(self.mainSplitter)
        
        # Set equal sizes for the sections in the main splitter (1:1 ratio)
        # The content splitter (section 1) will get 2/3, status box (section 2) will get 1/3
        self.mainSplitter.setSizes([2, 1])

        self.retranslateUi()

    def retranslateUi(self):
        """Set up UI text."""
        self.titleLabel.setStyleSheet("font-size: 18pt; font-weight: bold;")
        self.titleLabel.setText(f"Queue Monitor v{VERSION}")

class TabContent(QWidget):
    def __init__(self):
        """Initialize the TabContent widget."""
        super().__init__()

        # Set up UI
        self.ui = Ui_TabContent()
        self.ui.setupUi(self)

        # Start JVM thread
        self.jvm_thread = JVMThread()
        self.jvm_thread.error_signal.connect(self.log_error)
        self.jvm_thread.log_signal.connect(self.log_message)
        self.jvm_thread.update_queues_signal.connect(self.update_queues_list)
        self.jvm_thread.update_topics_signal.connect(self.update_topics_list)
        self.jvm_thread.update_messages_signal.connect(self.update_messages_table)

        # Connect signals
        self.ui.connectButton.clicked.connect(self.connect_to_broker)
        self.ui.refreshButton.clicked.connect(self.refresh_destinations)
        self.ui.queuesList.itemClicked.connect(lambda item: self.destination_selected(item, 'queue'))
        self.ui.topicsList.itemClicked.connect(lambda item: self.destination_selected(item, 'topic'))
        self.ui.monitorButton.clicked.connect(self.start_monitoring)
        self.ui.monitorAllButton.clicked.connect(self.start_monitoring_all)
        self.ui.stopButton.clicked.connect(self.stop_monitoring)
        self.ui.sendButton.clicked.connect(self.prepare_new_message)
        self.ui.sendEditedButton.clicked.connect(self.send_edited_message)
        self.ui.messagesTable.itemClicked.connect(self.message_selected)
        self.ui.exportCsvButton.clicked.connect(self.export_to_csv)

        # Initialize jars path
        self.jars_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "MinimalOpenWires", "jars")
        
        # State
        self.current_destination = None
        self.current_destination_type = None
        self.current_message = None
        self.editing_new_message = False

        # Log initialization
        self.log_message(f"QueueMonitor v{VERSION} initialized")
        self.log_message(f"JAR path: {self.jars_path}")
        
        # Log Java information
        java_home = find_java_home()
        if java_home:
            self.log_message(f"Java Home: {java_home}")
            jvm_path = find_jvm_dll(java_home)
            if jvm_path:
                self.log_message(f"JVM Path: {jvm_path}")
            else:
                self.log_error("JVM not found. Please set JAVA_HOME correctly.")
        else:
            self.log_error("Java installation not found. Please install Java and set JAVA_HOME.")

    def log_debug(self, message):
        """Empty implementation of log_debug - used only for very detailed debugging
        
        This method is intentionally empty to prevent harmless table update log entries
        from filling the log window.
        """
        # Debugging is disabled in this version, so this method does nothing
        pass

    def log_error(self, message, critical=False):
        """Log an error message to the status text box
        
        Args:
            message: The error message to log
            critical: If True, always show regardless of importance
        """
        # Check for non-critical flag in message
        if "::non-critical" in message:
            message = message.replace("::non-critical", "")
            critical = False
            
        # Always log errors, they're important
        self.log_message(f"ERROR: {message}")
            
    def log_message(self, message):
        """Log a message to the status text box"""
        self.ui.statusTextBox.appendPlainText(message)
        # Ensure cursor is at the end and visible
        self.ui.statusTextBox.moveCursor(QTextCursor.End)
        self.ui.statusTextBox.ensureCursorVisible()
        # Force the scrollbar to update immediately
        QApplication.processEvents()

    def showEvent(self, event):
        """Handle window show event."""
        super().showEvent(event)
        
        # Check if JAR directory exists
        if not os.path.exists(self.jars_path):
            # Try to find the jars directory
            parent_dir = os.path.dirname(os.path.abspath(__file__))
            possible_paths = [
                os.path.join(parent_dir, "..", "jars"),
                os.path.join(parent_dir, "jars")
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    self.jars_path = path
                    self.log_message(f"Found JAR path: {self.jars_path}")
                    break
            else:
                self.log_error("JAR directory not found. Please select it manually.")
                self.select_jars_directory()

    def select_jars_directory(self):
        """Select the JAR directory manually"""
        dir_path = QFileDialog.getExistingDirectory(self, "Select JAR Directory")
        if dir_path:
            self.jars_path = dir_path
            self.log_message(f"JAR path set to: {self.jars_path}")
        else:
            self.log_error("No JAR directory selected. Some features may not work.")

    def connect_to_broker(self):
        """Connect to the ActiveMQ broker"""
        method = self.ui.methodCombo.currentText()
        host = self.ui.hostLine.text()
        port = self.ui.portLine.text()
        username = self.ui.usernameLine.text()
        password = self.ui.passwordLine.text()
        
        # Start JVM if not already started
        if not self.jvm_thread.start_jvm(self.jars_path):
            self.log_error("Failed to start JVM. Please check your Java installation.")
            # Add a helpful hint about ActiveMQ
            self.log_message("Tips:")
            self.log_message("1. Make sure ActiveMQ is running")
            self.log_message("2. You can start ActiveMQ using the LoadActiveMQ tab")
            self.log_message("3. Default ports are: 61616 (OpenWire), 1099 (JMX)")
            self.log_message("4. Wrong host/port - verify connection settings")
            return
        
        # Connect to broker
        self.log_message(f"Attempting to connect to {method}://{host}:{port}")
        success = self.jvm_thread.connect_to_broker(method, host, port, username, password)
        
        # If authentication failed, try common credentials
        if not success and "password is invalid" in self.ui.statusTextBox.toPlainText():
            self.log_message("Authentication failed. Trying common credentials...")
            common_credentials = [
                ("", ""),               # No credentials
                ("admin", "admin"),     # Default ActiveMQ
                ("admin", "activemq"),  # Another common ActiveMQ combo
                ("user", "password"),   # Generic
                ("guest", "guest"),     # RabbitMQ default
                ("system", "manager"),  # WebSphere MQ default
                (username, "")          # Username with empty password
            ]
            
            # Skip the first credential if it matches what we just tried
            if username == common_credentials[0][0] and password == common_credentials[0][1]:
                common_credentials.pop(0)
            if username == common_credentials[1][0] and password == common_credentials[1][1]:
                common_credentials.pop(1)
            
            # Try each credential
            for user, pwd in common_credentials:
                self.log_message(f"Trying with username='{user}' password='{pwd}'")
                success = self.jvm_thread.connect_to_broker(method, host, port, user, pwd)
                if success:
                    self.ui.usernameLine.setText(user)
                    self.ui.passwordLine.setText(pwd)
                    self.log_message(f"Connected successfully with username='{user}'")
                    break
        
        if success:
            self.ui.connectButton.setText("Disconnect")
            self.ui.connectButton.clicked.disconnect()
            self.ui.connectButton.clicked.connect(self.disconnect_from_broker)
            
            # Get initial destinations
            self.refresh_destinations()
            
            # Auto-read all queues
            self.auto_read_all_queues()
        else:
            self.log_error("Connection failed. Please check if ActiveMQ is running.")
            self.log_message("Common issues:")
            self.log_message("1. ActiveMQ not running - start it using LoadActiveMQ tab")
            self.log_message("2. Wrong credentials - check username/password")
            self.log_message("3. Firewall blocking connections - check firewall settings")
            self.log_message("4. Wrong host/port - verify connection settings")
    
    def auto_read_all_queues(self):
        """Automatically read all queues without entering monitoring mode"""
        self.log_message("Auto-reading all queues...")
        
        # Get current queues
        queues = []
        for i in range(self.ui.queuesList.count()):
            queues.append(self.ui.queuesList.item(i).text())
        
        # Browse each queue
        all_messages = []
        for queue_name in queues:
            self.log_message(f"Reading queue: {queue_name}")
            messages = self.jvm_thread.browse_queue(queue_name, update_ui=False)
            all_messages.extend(messages)
        
        # Update UI with all messages
        self.jvm_thread.update_messages_signal.emit(all_messages)
        self.log_message(f"Finished reading {len(queues)} queues, found {len(all_messages)} messages")
    
    def disconnect_from_broker(self):
        """Disconnect from the broker"""
        self.stop_monitoring()
        
        if self.jvm_thread.disconnect():
            self.ui.connectButton.setText("Connect")
            self.ui.connectButton.clicked.disconnect()
            self.ui.connectButton.clicked.connect(self.connect_to_broker)
            
            # Clear lists
            self.ui.queuesList.clear()
            self.ui.topicsList.clear()
            self.ui.messagesTable.setRowCount(0)
    
    def refresh_destinations(self):
        """Refresh the list of queues and topics"""
        self.jvm_thread.get_destinations()
    
    def update_queues_list(self, queues):
        """Update the queues list"""
        self.ui.queuesList.clear()
        for queue in queues:
            self.ui.queuesList.addItem(queue)
    
    def update_topics_list(self, topics):
        """Update the topics list"""
        self.ui.topicsList.clear()
        for topic in topics:
            self.ui.topicsList.addItem(topic)
    
    def update_messages_table(self, messages):
        """Update the messages table with the given messages, preserving existing data"""
        try:
            if not messages:
                return
            
            # Disable updates during processing
            self.ui.messagesTable.setUpdatesEnabled(False)
            
            # Build a set of existing message IDs
            existing_ids = set()
            for row in range(self.ui.messagesTable.rowCount()):
                item = self.ui.messagesTable.item(row, 0)
                if item and item.data(Qt.UserRole):
                    msg_data = item.data(Qt.UserRole)
                    existing_ids.add(msg_data['id'])
            
            # Find new messages that aren't already in the table
            new_messages = []
            for message in messages:
                if message['id'] not in existing_ids:
                    new_messages.append(message)
            
            # If no new messages, just exit
            if not new_messages and self.ui.messagesTable.rowCount() > 0:
                self.ui.messagesTable.setUpdatesEnabled(True)
                return
            
            # If table is empty, initialize headers
            if self.ui.messagesTable.rowCount() == 0:
                self.ui.messagesTable.setColumnCount(5)
                self.ui.messagesTable.setHorizontalHeaderLabels(["ID", "Destination", "Type", "Timestamp", "Body"])
            
            # Add only new messages to the existing table
            current_row_count = self.ui.messagesTable.rowCount()
            self.ui.messagesTable.setRowCount(current_row_count + len(new_messages))
            
            # Add new messages at the end
            for i, message in enumerate(new_messages):
                row = current_row_count + i
                try:
                    # ID
                    id_item = QTableWidgetItem(str(message['id'])[:20])
                    self.ui.messagesTable.setItem(row, 0, id_item)
                    
                    # Destination
                    dest = f"{message['type']}:{message['destination']}"
                    self.ui.messagesTable.setItem(row, 1, QTableWidgetItem(dest))
                    
                    # Type
                    self.ui.messagesTable.setItem(row, 2, QTableWidgetItem(str(message.get('message_type', 'unknown'))))
                    
                    # Timestamp
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(message['timestamp']) / 1000))
                    self.ui.messagesTable.setItem(row, 3, QTableWidgetItem(ts))
                    
                    # Body
                    body = str(message['body'])[:100]
                    if len(str(message['body'])) > 100:
                        body += "..."
                    self.ui.messagesTable.setItem(row, 4, QTableWidgetItem(body))
                    
                    # Store message data
                    id_item.setData(Qt.UserRole, message)
                    
                    # Log new topic messages
                    if message['type'] == 'topic':
                        self.log_debug(f"Added new topic message to row {row}: {message['id']}")
                    
                except Exception as e:
                    self.log_error(f"Error adding message to table: {str(e)}")
            
            # Resize columns
            self.ui.messagesTable.resizeColumnsToContents()
            self.ui.messagesTable.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
            
            # Scroll to bottom for new messages if any were added
            if new_messages:
                self.ui.messagesTable.scrollToBottom()
            
            # Re-enable updates
            self.ui.messagesTable.setUpdatesEnabled(True)
        except Exception as e:
            self.log_error(f"Error updating table: {str(e)}")
            self.ui.messagesTable.setUpdatesEnabled(True)
    
    def destination_selected(self, item, destination_type):
        """Handle destination selection"""
        destination_name = item.text()
        
        # Skip if already selected
        if (self.current_destination == destination_name and 
            self.current_destination_type == destination_type):
            return
            
        self.current_destination = destination_name
        self.current_destination_type = destination_type
        
        # Browse messages
        if destination_type == 'queue':
            self.jvm_thread.browse_queue(destination_name, force_new_browser=True)
        else:  # topic
            # For topics, create a subscription to receive future messages
            topic_key = f"topic:{destination_name}"
            
            # Check if we already have a listener for this topic
            if topic_key not in self.jvm_thread.topic_consumers:
                # Create new listener if none exists
                consumer, _ = self.jvm_thread.create_topic_message_listener(destination_name)
                if consumer:
                    self.log_message(f"Subscribed to topic {destination_name} - messages will appear when published")
            else:
                self.log_message(f"Already subscribed to topic {destination_name} - messages will appear when published")
                
            # Check if we have any cached messages for this topic to display
            with self.jvm_thread.message_cache_lock:
                if topic_key in self.jvm_thread.message_cache:
                    messages = self.jvm_thread.message_cache[topic_key]
                    if messages:
                        self.log_message(f"Found {len(messages)} cached messages for topic {destination_name}")
                        self.jvm_thread.update_messages_signal.emit(messages)
    
    def start_monitoring(self):
        """Start monitoring the selected destination"""
        if not self.current_destination:
            self.log_error("No destination selected", critical=True)
            return
        
        try:
            # Always use recursive mode for auto-refresh
            recursive = True
            
            self.jvm_thread.start_monitoring(
                self.current_destination, 
                self.current_destination_type,
                recursive
            )
            
            # Update UI
            self.ui.monitorButton.setEnabled(False)
            self.ui.monitorAllButton.setEnabled(False)
            self.ui.stopButton.setEnabled(True)
            
            # Update status label
            if self.current_destination_type == 'topic':
                self.ui.statusLabel.setText("Active Topic Subscription Enabled (Auto-refreshing)")
            else:
                self.ui.statusLabel.setText("Queue Monitoring Active (Auto-refreshing)")
        except Exception as e:
            self.log_error(f"Error starting monitoring: {str(e)}", critical=True)
            import traceback
            self.log_message(traceback.format_exc())
    
    def message_selected(self, item):
        """Handle message selection"""
        try:
            row = item.row()
            id_item = self.ui.messagesTable.item(row, 0)
            if id_item:
                self.current_message = id_item.data(Qt.UserRole)
                
                # Display message in editor
                self.ui.messageEditor.setText(str(self.current_message['body']))
                
                # Display properties
                properties_str = ",".join([f"{str(k)}={str(v)}" for k, v in self.current_message['properties'].items()])
                self.ui.propertiesLine.setText(properties_str)
                
                self.editing_new_message = False
        except Exception as e:
            self.log_error(f"Error selecting message: {str(e)}")
            import traceback
            self.log_message(traceback.format_exc())
    
    def prepare_new_message(self):
        """Prepare to send a new message"""
        if not self.current_destination:
            self.log_error("No destination selected")
            return
        
        # Clear editor
        self.ui.messageEditor.clear()
        self.ui.propertiesLine.clear()
        
        self.editing_new_message = True
        self.log_message(f"Preparing new message for {self.current_destination_type} {self.current_destination}")
    
    def send_edited_message(self):
        """Send the edited message"""
        try:
            if not self.current_destination:
                self.log_error("No destination selected", critical=True)
                return
            
            # Get message body and properties
            body = self.ui.messageEditor.toPlainText()
            properties_str = self.ui.propertiesLine.text()
            
            # Parse properties
            properties = {}
            if properties_str:
                for prop in properties_str.split(","):
                    if "=" in prop:
                        key, value = prop.split("=", 1)
                        properties[key.strip()] = value.strip()
            
            # Send message - jvm_thread.send_message already logs success
            success = self.jvm_thread.send_message(
                self.current_destination,
                self.current_destination_type,
                body,
                properties
            )
            
            # For topics with active listeners, we don't need to refresh
            # as the listener will handle new messages automatically
            if success and self.current_destination_type == 'queue':
                # Only refresh queues - topics are automatically handled by listeners
                self.jvm_thread.browse_queue(self.current_destination, auto_refresh=True)
            
            if not success:
                self.log_error(f"Failed to send message", critical=True)
        except Exception as e:
            self.log_error(f"Error sending message: {str(e)}", critical=True)
            import traceback
            self.log_message(traceback.format_exc())
    
    def start_monitoring_all(self):
        """Start monitoring all queues and topics"""
        try:
            # Always use recursive mode for auto-refresh
            recursive = True
            
            # Get all available destinations
            queues, topics = self.jvm_thread.get_destinations()
            
            if not queues and not topics:
                self.log_error("No destinations found to monitor", critical=True)
                return
                
            # Use the new monitor_all method
            self.jvm_thread.monitor_all(queues, topics, recursive)
            
            # Update UI
            self.ui.monitorButton.setEnabled(False)
            self.ui.monitorAllButton.setEnabled(False)
            self.ui.stopButton.setEnabled(True)
            
            # Update status label
            if topics:
                self.ui.statusLabel.setText(f"Monitoring {len(queues)} Queues & {len(topics)} Topics (Auto-refreshing)")
            else:
                self.ui.statusLabel.setText(f"Monitoring {len(queues)} Queues (Auto-refreshing)")
            
            self.log_message("=====================================================")
            self.log_message(f"MONITORING ALL ACTIVATED: Checking all destinations")
            if queues:
                self.log_message(f"QUEUES: {', '.join(queues)}")
            if topics:
                self.log_message(f"TOPICS: {', '.join(topics)}")
            self.log_message("New messages will appear in the table as they arrive")
            self.log_message("=====================================================")
        except Exception as e:
            self.log_error(f"Error starting monitoring all: {str(e)}", critical=True)
            import traceback
            self.log_message(traceback.format_exc())
    
    def stop_monitoring(self):
        """Stop monitoring"""
        try:
            # Force UI update
            self.ui.monitorButton.setEnabled(True)
            self.ui.monitorAllButton.setEnabled(True)
            self.ui.stopButton.setEnabled(False)
            self.ui.statusLabel.setText("")
            
            # Direct approach - cancel all monitoring with minimal thread interaction
            self.jvm_thread.emergency_stop_monitoring()
        except Exception as e:
            self.log_error(f"Error stopping monitoring: {str(e)}", critical=True)

    def export_to_csv(self):
        """Export the current message table data to a CSV file
        
        This includes all message data including properties.
        """
        try:
            # Check if there are any messages to export
            if self.ui.messagesTable.rowCount() == 0:
                self.log_error("No messages to export")
                return
            
            # Open file dialog to select save location
            options = QFileDialog.Options()
            filepath, _ = QFileDialog.getSaveFileName(
                self,
                "Save Messages as CSV",
                os.path.expanduser("~/messages_export.csv"),
                "CSV Files (*.csv)",
                options=options
            )
            
            if not filepath:  # User canceled
                return
            
            # Add .csv extension if not present
            if not filepath.lower().endswith('.csv'):
                filepath += '.csv'
            
            # Collect all messages with their properties
            messages = []
            for row in range(self.ui.messagesTable.rowCount()):
                id_item = self.ui.messagesTable.item(row, 0)
                if id_item and id_item.data(Qt.UserRole):
                    msg_data = id_item.data(Qt.UserRole)
                    messages.append(msg_data)
            
            # Open CSV file for writing
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                # Set up CSV writer
                fieldnames = ['id', 'destination', 'type', 'message_type', 'timestamp', 'body']
                
                # Add all unique property names from all messages
                property_names = set()
                for msg in messages:
                    for prop_name in msg.get('properties', {}).keys():
                        property_names.add(prop_name)
                
                # Add property names to fieldnames
                for prop_name in sorted(property_names):
                    fieldnames.append(f'property_{prop_name}')
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write message data
                for msg in messages:
                    # Create row dictionary
                    row_data = {
                        'id': msg.get('id', ''),
                        'destination': msg.get('destination', ''),
                        'type': msg.get('type', ''),
                        'message_type': msg.get('message_type', ''),
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(msg.get('timestamp', 0)) / 1000)),
                        'body': msg.get('body', '').replace('\n', '\\n'),  # Escape newlines for CSV
                    }
                    
                    # Add properties
                    for prop_name, prop_value in msg.get('properties', {}).items():
                        row_data[f'property_{prop_name}'] = prop_value
                    
                    writer.writerow(row_data)
            
            self.log_message(f"Exported {len(messages)} messages to {filepath}")
            
        except Exception as e:
            self.log_error(f"Error exporting to CSV: {str(e)}")
            import traceback
            self.log_error(traceback.format_exc())

    def cleanup(self):
        """Clean up resources when closing"""
        self.stop_monitoring()
        self.jvm_thread.disconnect()
        
        # Shutdown JVM if it was started
        if jpype.isJVMStarted():
            jpype.shutdownJVM() 