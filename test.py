import paho.mqtt.client as mqtt
import time
import random
import datetime
import pytz
import ssl
import argparse
import os
import threading
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

# MQTT Configuration
MQTT_BROKER = os.environ.get("MQTT_BROKER")
MQTT_PORT = int(os.environ.get("MQTT_PORT"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")

# MQTT Topics
topic1 = "tank/topic1"  # Left Tank (Girls) level (cm)
topic2 = "tank/topic2"  # Left Tank (Girls) volume (liters)
topic3 = "tank/topic3"  # Right Tank (Boys) level (cm)
topic4 = "tank/topic4"  # Right Tank (Boys) volume (liters)
topic5 = "tank/topic5"  # Timestamp (IST)
lwt_topic = "tank/status"  # LWT topic
lwt_message = "Offline"  # LWT message
HEARTBEAT_TOPIC = "tank/heartbeat"  # Heartbeat topic

# Missing connection configuration variables
HEARTBEAT_INTERVAL = 60  # Send heartbeat every 60 seconds
RECONNECT_BASE_DELAY = 5  # Initial reconnect delay in seconds
RECONNECT_MAX_DELAY = 300  # Maximum reconnect delay in seconds (5 minutes)

# Connection status tracking variables
mqtt_client_connected = False
last_message_time = time.time()
current_reconnect_delay = RECONNECT_BASE_DELAY
last_successful_connection = 0

# Tank Properties
TANK_AREA_CM2 = 91628.57  # Cross-sectional area of both tanks (cm²)
MAX_LEVEL_CM = 140  # Max water level (cm)
MIN_LEVEL_CM = 2    # Min water level (cm)
REFILL_DURATION = 55   # Minutes to refill both tanks (1 hour)

# Initial simulation time (will be set from command line or default to current time)
simulation_time = None

# Initial Water Levels
left_tank_level = 80  # Girls tank starting level
right_tank_level = 75  # Boys tank starting level

# Shared refill state tracking for both tanks
tanks_refilling = False
refill_start_time = None
left_refill_start_level = 0
right_refill_start_level = 0
last_refill_day = None  # Track the last day a refill was initiated

# Student Distribution
STUDENTS_PER_FLOOR = {
    "ground": 180,  # ~60 students per year × 3 years + staff
    "first": 180,   # Similar to ground floor (IT department)
    "third": 72     # ~40% of the count on other floors
}

# Usage ranges for all periods (will be randomized every 3 minutes)
USAGE_PERCENT_RANGES = {
    "morning_break": {
        "girls": {"min": 40.0, "max": 50.0, "center": 45.0},
        "boys": {"min": 40.0, "max": 55.0, "center": 50.0}
    },
    "lunch_break": {
        "girls": {"min": 65.0, "max": 75.0, "center": 67.0},
        "boys": {"min": 70.0, "max": 77.0, "center": 70.0}
    },
    "evening_break": {
        "girls": {"min": 55.0, "max": 65.0, "center": 50.0},
        "boys": {"min": 50.0, "max": 65.0, "center": 57.0}
    },
    "evening_classes": {
        "girls": {"min": 0.8, "max": 1.2, "center": 1.0},
        "boys": {"min": 0.8, "max": 1.2, "center": 1.0}
    },
    "regular": {
        "girls": {"min": 0.1, "max": 1, "center": 0.5},
        "boys": {"min": 0.1, "max": 1.2, "center": 0.6}
    },
    "night_time": {
        "girls": {"min": 0.90, "max": 2.2, "center": 1.5},
        "boys": {"min": 0.95, "max": 2.1, "center": 1.6}
    },
    "early_morning": {
        "girls": {"min": 0.60, "max": 0.80, "center": 0.70},
        "boys": {"min": 0.65, "max": 0.85, "center": 0.75}
    }
}

# Store current variations for all periods
current_usage_variations = {}

# Water usage per student (liters)
WATER_PER_USE = {
    "girls": 3.5,  # Slightly more water usage for girls' restrooms
    "boys": 3.0    # Water usage for boys' restrooms
}

# IST Timezone
IST = pytz.timezone("Asia/Kolkata")

# ===== HTTP Server for Health Checks =====
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        status = "Connected" if mqtt_client_connected else "Disconnected"
        self.wfile.write(f"Tank Simulation is running (Status: {status})".encode())
        
    def log_message(self, format, *args):
        # Suppress logs from HTTP requests to avoid cluttering the console
        return

def start_health_server():
    port = int(os.environ.get('PORT', 8080))  # Render assigns a PORT env variable
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"Starting health check server on port {port}")
    server.serve_forever()

# ===== MQTT Connection Functions =====
def heartbeat_check():
    """Function to periodically check connection health and send heartbeats"""
    global last_message_time
    
    while True:
        current_time = time.time()
        time_since_last_message = current_time - last_message_time
        
        # Check if we've received any message recently
        if time_since_last_message > HEARTBEAT_INTERVAL * 2:
            print(f"WARNING: No messages or activity for {time_since_last_message:.1f} seconds")
        
        # Send a heartbeat if connected, otherwise try to reconnect
        if mqtt_client_connected:
            try:
                timestamp = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
                client.publish(HEARTBEAT_TOPIC, f"HEARTBEAT: {timestamp}")
                print(f"Heartbeat sent at {timestamp}")
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
                # Try to reconnect
                reconnect()
        else:
            print("Not connected to MQTT broker during heartbeat check, attempting to reconnect...")
            reconnect()
        
        # Sleep until next heartbeat
        time.sleep(HEARTBEAT_INTERVAL)

def reconnect():
    """Attempt to reconnect with exponential backoff"""
    global current_reconnect_delay, mqtt_client_connected
    
    if mqtt_client_connected:
        print("Reconnect called while already connected. Skipping.")
        return
    
    print(f"Attempting to reconnect in {current_reconnect_delay} seconds...")
    time.sleep(current_reconnect_delay)
    
    try:
        client.reconnect()
        print("Reconnection successful")
        # Reset the reconnect delay on successful connection
        current_reconnect_delay = RECONNECT_BASE_DELAY
    except Exception as e:
        print(f"Reconnection failed: {e}")
        # Increase the delay for next attempt (exponential backoff)
        current_reconnect_delay = min(current_reconnect_delay * 2, RECONNECT_MAX_DELAY)
        # Schedule another reconnect attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

def on_connect(client, userdata, flags, reason_code, properties=None):
    global mqtt_client_connected, last_successful_connection, current_reconnect_delay
    
    if reason_code == 0:
        mqtt_client_connected = True
        last_successful_connection = time.time()
        current_reconnect_delay = RECONNECT_BASE_DELAY  # Reset backoff on successful connection
        print(f"Connected to MQTT broker successfully at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        mqtt_client_connected = False
        print(f"Failed to connect to MQTT broker. Return code: {reason_code}")
        # Schedule a reconnection attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

def on_disconnect(client, userdata, reason_code, properties=None, mid=None):
    global mqtt_client_connected
    
    mqtt_client_connected = False
    print(f"Disconnected from MQTT broker with code {reason_code} at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if reason_code != 0:
        print("Unexpected disconnection, scheduling reconnect...")
        # Schedule a reconnection attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

# ===== Tank Simulation Functions =====
def calculate_volume(level_cm):
    """Calculate water volume in liters."""
    volume_cm3 = TANK_AREA_CM2 * level_cm
    return volume_cm3 / 1000  # Convert cm³ to liters

def is_weekday(current_time):
    """Check if current day is a weekday (Monday-Friday)."""
    return current_time.weekday() < 5  # 0-4 are Monday to Friday

def get_current_break_period(current_time):
    """Determine which break period we're in, if any."""
    hour, minute = current_time.hour, current_time.minute
    time_in_minutes = hour * 60 + minute
    
    # Night time (10PM - 5AM)
    if 22*60 <= time_in_minutes or time_in_minutes < 6*60:
        return "night_time"
    
    # Early morning (5AM - 8AM)
    elif 6*60 <= time_in_minutes < 9*60:
        return "early_morning"
    
    # Morning break (10:30-11:00 AM)
    elif 10*60+30 <= time_in_minutes < 11*60:
        return "morning_break"
    
    # Lunch break (12:30-1:30 PM)
    elif 12*60+30 <= time_in_minutes < 13*60+30:
        return "lunch_break"
    
    # Evening break (3:50-4:10 PM)
    elif 15*60+50 <= time_in_minutes < 16*60+10:
        return "evening_break"
    
    # Evening classes (6:00-9:00 PM) - no random chance, always classified as evening classes
    elif 17*60 <= time_in_minutes < 22*60 and is_weekday(current_time):
        return "night_time"
    
    # Regular hours
    else:
        return "regular"

def update_all_usage_variations():
    """Update usage variations for all periods (called every 3 minutes)."""
    global current_usage_variations
    
    # Initialize if empty
    if not current_usage_variations:
        current_usage_variations = {period: {"girls": 0, "boys": 0} for period in USAGE_PERCENT_RANGES}
    
    # Generate new random values within the specified ranges for all periods
    for period, genders in USAGE_PERCENT_RANGES.items():
        for gender, range_values in genders.items():
            min_val = range_values["min"]
            max_val = range_values["max"]
            center = range_values["center"]
            
            # Use different randomization approaches based on the period type
            if period in ["night_time", "early_morning", "evening_classes"]:
                # For very low usage periods, use narrower variations
                if random.random() < 0.7:
                    # 70% of the time stay closer to center
                    variance = (max_val - min_val) * 0.3  # 30% of range
                    current_usage_variations[period][gender] = random.uniform(
                        max(min_val, center - variance),
                        min(max_val, center + variance)
                    )
                else:
                    # 30% of the time, use wider range
                    current_usage_variations[period][gender] = random.triangular(
                        min_val, max_val, center
                    )
            
            elif period in ["morning_break", "lunch_break", "evening_break"]:
                # For scheduled breaks, use triangular distribution centered on typical value
                # This creates more realistic patterns for scheduled activities
                current_usage_variations[period][gender] = random.triangular(
                    min_val, max_val, center
                )
            
            else:  # regular hours
                # For regular hours, more unpredictable patterns
                if random.random() < 0.6:
                    # 60% of the time triangular with mode in first third
                    mode = min_val + (max_val - min_val) * 0.3
                    current_usage_variations[period][gender] = random.triangular(
                        min_val, max_val, mode
                    )
                else:
                    # 40% of the time uniform distribution
                    current_usage_variations[period][gender] = random.uniform(
                        min_val, max_val
                    )
    
    return current_usage_variations

def calculate_consumption(tank_type, current_break, current_time):
    """Calculate realistic water consumption based on time, day, and tank."""
    # Get usage percentage from current variations
    usage_percent = current_usage_variations[current_break][tank_type]
    
    # For weekends, just random fluctuations (no consistent decrease)
    if not is_weekday(current_time) and current_break not in ["night_time", "early_morning"]:
        # Keep a very small chance of minimal usage
        if random.random() < 0.05:  # 5% chance of minimal activity
            # Very small consumption
            base_rate = 0.02  # Very small base rate
            return base_rate * random.triangular(0.5, 1.5, 1.0)
        else:
            # Random fluctuation centered around zero to avoid leak-like pattern
            return random.triangular(-0.008, 0.008, 0.0)
    
    # For very low consumption periods (night, early morning)
    if current_break in ["night_time", "early_morning"]:
        # Keep variations but still mostly fluctuations
        if random.random() < 0.85:  # 85% of the time, just sensor noise
            return random.triangular(-0.006, 0.006, 0.0)
        else:  # 15% of the time, tiny usage based on percentage
            # Convert percentage to actual consumption with variation
            users = sum(STUDENTS_PER_FLOOR.values()) * (usage_percent / 100) * 0.05
            water_used = users * WATER_PER_USE[tank_type] * random.uniform(0.8, 1.2)
            level_change = water_used * 1000 / TANK_AREA_CM2  # L to cm³ to cm
            
            # Ensure the consumption is small and not following a leak pattern
            # by adding some random fluctuation around the calculated value
            return level_change * random.triangular(0.7, 1.3, 1.0)
    
    # For evening classes with very low usage
    if current_break == "evening_classes" and usage_percent < 5:
        # More randomized pattern with occasional use
        if random.random() < 0.4:  # 40% chance of some minimal activity
            users = sum(STUDENTS_PER_FLOOR.values()) * (usage_percent / 100) * random.uniform(0.8, 1.2)
            water_used = users * WATER_PER_USE[tank_type] * random.uniform(0.9, 1.1)
            level_change = water_used * 1000 / TANK_AREA_CM2
            return level_change * (3 / 180)  # Scaled for 3-minute interval
        else:
            # Just sensor noise
            return random.triangular(-0.007, 0.007, 0.0)
    
    # For high-usage break periods and regular hours, calculate based on student distribution
    total_consumption = 0
    
    for floor, students in STUDENTS_PER_FLOOR.items():
        floor_factor = 1.0  # Default factor
        if floor == "third":
            floor_factor = 0.8  # Less traffic on third floor
        
        # Add day-of-week factor (e.g., Mondays and Fridays might differ from midweek)
        weekday = current_time.weekday()
        if weekday == 0:  # Monday
            day_factor = random.uniform(0.95, 1.05)  # Slight variation for Monday
        elif weekday == 4:  # Friday
            day_factor = random.uniform(0.92, 1.02)  # Potentially slightly lower on Friday
        else:  # Tuesday-Thursday
            day_factor = random.uniform(0.98, 1.07)  # Potentially slightly higher midweek
        
        # Add time-of-day factor for regular hours to create natural patterns
        if current_break == "regular":
            hour = current_time.hour
            # More activity around 9am and 2pm during regular hours
            if hour in [9, 14]:
                time_factor = random.uniform(1.1, 1.25)
            # Less activity around mid-morning or late afternoon
            elif hour in [10, 17]:
                time_factor = random.uniform(0.8, 0.95)
            # Normal activity otherwise
            else:
                time_factor = random.uniform(0.95, 1.05)
        else:
            time_factor = 1.0
        
        # Number of students using restroom on this floor during this break
        users = students * (usage_percent / 100) * floor_factor * day_factor * time_factor
        
        # Randomize a bit for realism
        users = users * random.uniform(0.85, 1.15)
        
        # Calculate water usage with some variation
        water_used = users * WATER_PER_USE[tank_type] * random.uniform(0.9, 1.1)
        
        total_consumption += water_used
    
    # Convert water usage in liters to level change in cm
    level_change = total_consumption * 1000 / TANK_AREA_CM2  # L to cm³ to cm
    
    # Add more randomness for realism and to break any linear pattern
    # Use a triangular distribution for more natural variation
    level_change = level_change * random.triangular(0.85, 1.15, 1.0)
    
    # Scale consumption to 3-minute intervals
    if current_break == "morning_break":
        return level_change * (3 / 30)  # 30 minute break
    elif current_break == "lunch_break":
        return level_change * (3 / 60)  # 60 minute break
    elif current_break == "evening_break":
        return level_change * (3 / 20)  # 20 minute break
    elif current_break == "evening_classes":
        return level_change * (3 / 180)  # 180 minute period
    else:
        return level_change  # Already scaled for regular periods

def check_and_handle_refill(current_time):
    """Handle shared refill logic for both tanks."""
    global tanks_refilling, refill_start_time, last_refill_day
    global left_refill_start_level, right_refill_start_level
    global left_tank_level, right_tank_level
    
    hour, minute = current_time.hour, current_time.minute
    time_in_minutes = hour * 60 + minute
    current_day = current_time.date()
    
    # Refill timing window: 9:00 AM to 9:20 AM on weekdays (Monday-Friday)
    refill_window = (9*60 <= time_in_minutes < 9*60+20) and is_weekday(current_time)
    
    if not tanks_refilling:
        # If we're in the refill window and we haven't refilled today
        if refill_window and last_refill_day != current_day:
            # Randomly decide if we start refill now - creates variation in start times within window
            if random.random() < 0.3:  # 30% chance to start if conditions are met
                print(f"Starting simultaneous refill for both tanks at {current_time.strftime('%H:%M:%S')}")
                print(f"Left Tank starting level: {left_tank_level:.2f} cm")
                print(f"Right Tank starting level: {right_tank_level:.2f} cm")
                
                # Save start levels for both tanks
                left_refill_start_level = left_tank_level
                right_refill_start_level = right_tank_level
                
                # Update shared refill state
                tanks_refilling = True
                refill_start_time = current_time
                last_refill_day = current_day
                
                return True
        
        return False
    
    else:  # Tanks are currently refilling
        # Calculate how far through the refill we are
        elapsed_minutes = (current_time - refill_start_time).total_seconds() / 60
        
        if elapsed_minutes >= REFILL_DURATION:
            # Refill completed
            print(f"Refill completed for both tanks at {current_time.strftime('%H:%M:%S')}")
            tanks_refilling = False
            refill_start_time = None
            return False
        
        # Calculate new levels during refill with a small shared randomness
        # This ensures both tanks refill at slightly variable but similar rates
        shared_random_factor = random.uniform(0.97, 1.03)
        
        # Calculate left tank new level
        left_level_increase = (MAX_LEVEL_CM - left_refill_start_level) * (elapsed_minutes / REFILL_DURATION)
        left_level_increase = left_level_increase * shared_random_factor
        left_tank_level = min(left_refill_start_level + left_level_increase, MAX_LEVEL_CM)
        
        # Calculate right tank new level
        right_level_increase = (MAX_LEVEL_CM - right_refill_start_level) * (elapsed_minutes / REFILL_DURATION)
        right_level_increase = right_level_increase * shared_random_factor
        right_tank_level = min(right_refill_start_level + right_level_increase, MAX_LEVEL_CM)
        
        # Print status occasionally to avoid cluttering the output
        if int(elapsed_minutes) % 10 == 0 and int(elapsed_minutes) > 0:
            print(f"Both tanks refilling: {int(elapsed_minutes)}/{REFILL_DURATION} minutes")
            print(f"Left Tank level: {left_tank_level:.2f} cm")
            print(f"Right Tank level: {right_tank_level:.2f} cm")
        
        return True

# ===== Tank Simulation Main Function =====
def run_tank_simulation():
    """Main function to run the tank simulation and publish data"""
    global left_tank_level, right_tank_level, last_message_time
    
    print("Starting Tank Simulation...")
    print(f"Current time: {datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize usage variations for all periods
    update_all_usage_variations()
    
    last_day = datetime.datetime.now(IST).day
    update_counter = 0
    
    while True:
        try:
            # Get current time in IST
            current_time = datetime.datetime.now(IST)
            update_counter += 1
            
            # Update last message time to show activity
            last_message_time = time.time()
            
            # Check if we've moved to a new day
            if current_time.day != last_day:
                last_day = current_time.day
                print(f"\nNew day: {current_time.strftime('%Y-%m-%d')}")
            
            # Get current break period
            current_break = get_current_break_period(current_time)
            
            # Update all usage variations every 3 minutes
            update_all_usage_variations()
            
            # Print variation changes occasionally to avoid cluttering
            if update_counter % 5 == 0:
                print("\nUpdated usage variations:")
                for period, genders in current_usage_variations.items():
                    if period == current_break:
                        print(f"  *{period}: Girls {genders['girls']:.2f}%, Boys {genders['boys']:.2f}%")
                    else:
                        print(f"  {period}: Girls {genders['girls']:.2f}%, Boys {genders['boys']:.2f}%")
            
            # Handle shared refill for both tanks
            is_refilling = check_and_handle_refill(current_time)
            
            if not is_refilling:
                # If not refilling, calculate consumption for both tanks
                left_consumption = calculate_consumption("girls", current_break, current_time)
                right_consumption = calculate_consumption("boys", current_break, current_time)
                
                # Update tank levels
                left_tank_level = max(min(left_tank_level - left_consumption, MAX_LEVEL_CM), MIN_LEVEL_CM)
                right_tank_level = max(min(right_tank_level - right_consumption, MAX_LEVEL_CM), MIN_LEVEL_CM)
            
            # Calculate volumes
            left_volume = calculate_volume(left_tank_level)
            right_volume = calculate_volume(right_tank_level)
            
            # Format timestamp to match exact ISO format
            timestamp = current_time.isoformat()
            
            # Only publish if connected
            if mqtt_client_connected:
                # Publish data to MQTT topics
                client.publish(topic1, round(left_tank_level, 2))
                client.publish(topic2, round(left_volume, 2))
                client.publish(topic3, round(right_tank_level, 2))
                client.publish(topic4, round(right_volume, 2))
                client.publish(topic5, timestamp)
                
                # Print status information
                print("\n" + "="*50)
                print(f"Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} {'Weekday' if is_weekday(current_time) else 'Weekend'}")
                print(f"Current period: {current_break}")
                print(f"Usage %: Girls {current_usage_variations[current_break]['girls']:.2f}%, Boys {current_usage_variations[current_break]['boys']:.2f}%")
                
                # Calculate consumption in milliliters for display
                if not is_refilling:
                    left_consumption_ml = left_consumption * TANK_AREA_CM2 / 1000
                    right_consumption_ml = right_consumption * TANK_AREA_CM2 / 1000
                    print(f"Left Tank (Girls) - Level: {left_tank_level:.2f} cm, Volume: {left_volume:.2f} L, Last change: {left_consumption_ml:.0f} ml")
                    print(f"Right Tank (Boys) - Level: {right_tank_level:.2f} cm, Volume: {right_volume:.2f} L, Last change: {right_consumption_ml:.0f} ml")
                else:
                    print(f"Left Tank (Girls) - Level: {left_tank_level:.2f} cm, Volume: {left_volume:.2f} L (Refilling)")
                    print(f"Right Tank (Boys) - Level: {right_tank_level:.2f} cm, Volume: {right_volume:.2f} L (Refilling)")
                
                print("Data Published!")
                print("="*50)
            else:
                print(f"Not connected to MQTT broker. Tank levels - Left: {left_tank_level:.2f} cm, Right: {right_tank_level:.2f} cm")
            
            # Sleep for 3 minutes (180 seconds)
            time.sleep(180)
        
        except Exception as e:
            print(f"Error in simulation loop: {e}")
            # Continue the loop even if there's an error
            time.sleep(30)

# ===== Main Execution =====
try:
    # Create an MQTT client instance with VERSION2 API
    client = mqtt.Client(client_id=f"tank-simulator-{int(time.time())}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    
    # Set the callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    
    # Set up authentication
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    # Set Last Will and Testament (LWT)
    client.will_set(lwt_topic, lwt_message, qos=1, retain=True)
    
    # Enable TLS/SSL
    client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
    
    # Set a longer keepalive interval
    client.keepalive = 360  # 6 minutes
    
    # First, start the health check server for Railway/Render
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    print("Health check server started")
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat_check, daemon=True)
    heartbeat_thread.start()
    print(f"Heartbeat monitor started with {HEARTBEAT_INTERVAL} second interval")
    
    # Resolve the hostname before connecting
    try:
        print(f"Resolving hostname for {MQTT_BROKER}...")
        ip_address = socket.gethostbyname(MQTT_BROKER)
        print(f"Resolved to IP: {ip_address}")
    except Exception as e:
        print(f"Warning: Could not resolve MQTT broker hostname: {e}")
    
    # Connect to MQTT broker
    print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=360)
    
    # Start the network loop in a non-blocking way
    client.loop_start()
    
    # Start the tank simulation in a separate thread
    simulation_thread = threading.Thread(target=run_tank_simulation, daemon=True)
    simulation_thread.start()
    
    # Main program loop
    print("Tank simulation is running. Press Ctrl+C to exit.")
    while True:
        time.sleep(60)
        # Check connection status periodically in the main thread
        if not mqtt_client_connected:
            print("Main loop detected client is not connected")
            if time.time() - last_successful_connection > 600:  # 10 minutes
                print("No connection for extended period, forcing new connection attempt...")
                try:
                    client.disconnect()
                except:
                    pass
                time.sleep(2)
                try:
                    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=360)
                except Exception as e:
                    print(f"Reconnection from main loop failed: {e}")
    
except KeyboardInterrupt:
    print("\nProgram terminated by user")
    client.publish(lwt_topic, "Manually Disconnected", qos=1, retain=True)
    client.loop_stop()
    client.disconnect()
    print("Disconnected from MQTT")
except Exception as e:
    print(f"Error: {e}")
    client.loop_stop()
    client.disconnect()