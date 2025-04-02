import paho.mqtt.client as mqtt
import time
import random
import datetime
import pytz
import ssl
import argparse
import os
import logging
import sys
from logging.handlers import RotatingFileHandler
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("tank_monitor.log", maxBytes=10485760, backupCount=5),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("tank_monitor")

# MQTT Configuration from environment variables
MQTT_BROKER = os.environ.get("MQTT_BROKER", "f449892e7b6e4850ae929bf1e722fef1.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 8883))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "maadunson128")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "Ipradeep@1")

# MQTT Topics
topic1 = os.environ.get("TOPIC1", "tank/topic1")  # Left Tank (Girls) level (cm)
topic2 = os.environ.get("TOPIC2", "tank/topic2")  # Left Tank (Girls) volume (liters)
topic3 = os.environ.get("TOPIC3", "tank/topic3")  # Right Tank (Boys) level (cm)
topic4 = os.environ.get("TOPIC4", "tank/topic4")  # Right Tank (Boys) volume (liters)
topic5 = os.environ.get("TOPIC5", "tank/topic5")  # Timestamp (IST)
lwt_topic = os.environ.get("LWT_TOPIC", "tank/status")  # LWT topic
lwt_message = os.environ.get("LWT_MESSAGE", "Offline")  # LWT message

# Tank Properties
TANK_AREA_CM2 = float(os.environ.get("TANK_AREA_CM2", 91628.57))  # Cross-sectional area of both tanks (cm²)
MAX_LEVEL_CM = float(os.environ.get("MAX_LEVEL_CM", 140))  # Max water level (cm)
MIN_LEVEL_CM = float(os.environ.get("MIN_LEVEL_CM", 2))    # Min water level (cm)
REFILL_DURATION = int(os.environ.get("REFILL_DURATION", 55))   # Minutes to refill both tanks

# Initial simulation time (will be set from command line or default to current time)
simulation_time = None

# Initial Water Levels
left_tank_level = float(os.environ.get("INITIAL_LEFT_LEVEL", 80))  # Girls tank starting level
right_tank_level = float(os.environ.get("INITIAL_RIGHT_LEVEL", 75))  # Boys tank starting level

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
        "girls": {"min": 0.1, "max": 8.0, "center": 2.5},
        "boys": {"min": 0.1, "max": 7.5, "center": 2.9}
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

# Web server status data - updated by the main process
status_data = {
    "status": "starting",
    "last_update": None,
    "mqtt_connected": False,
    "error": None
}

# HTTP Handler for web interface
class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global left_tank_level, right_tank_level, simulation_time, tanks_refilling
        
        if self.path == '/health' or self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            # Create status report
            status = {
                "status": status_data["status"],
                "timestamp": datetime.datetime.now(IST).isoformat(),
                "simulation_time": simulation_time.isoformat() if simulation_time else None,
                "mqtt_connected": status_data["mqtt_connected"],
                "last_update": status_data["last_update"],
                "tanks": {
                    "left": {
                        "level_cm": round(left_tank_level, 2),
                        "volume_liters": round(calculate_volume(left_tank_level), 2)
                    },
                    "right": {
                        "level_cm": round(right_tank_level, 2),
                        "volume_liters": round(calculate_volume(right_tank_level), 2)
                    }
                },
                "refilling": tanks_refilling,
                "current_period": get_current_break_period(datetime.datetime.now(IST)),
                "render_deployment_time": "2025-04-02 17:16:10"
            }
            
            # Add any error information
            if status_data["error"]:
                status["error"] = status_data["error"]
            
            self.wfile.write(json.dumps(status, indent=2).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def log_message(self, format, *args):
        # Redirect server logs to our logger
        if args[1] != '/health':  # Don't log health check requests to reduce noise
            logger.info(f"HTTP: {args[0]} {args[1]} {args[2]}")

def start_web_server():
    """Start the web server in a separate thread."""
    port = int(os.environ.get('PORT', 10000))
    server = HTTPServer(('0.0.0.0', port), StatusHandler)
    logger.info(f"Starting web server on port {port}")
    server.serve_forever()

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
    
    # Refill timing window: 9:00 AM to 9:40 AM on weekdays (Monday-Friday)
    refill_window = (9*60 <= time_in_minutes < 9*60+20) and is_weekday(current_time)
    
    if not tanks_refilling:
        # If we're in the refill window and we haven't refilled today
        if refill_window and last_refill_day != current_day:
            # Randomly decide if we start refill now - creates variation in start times within window
            if random.random() < 0.3:  # 30% chance to start if conditions are met
                logger.info(f"Starting simultaneous refill for both tanks at {current_time.strftime('%H:%M:%S')}")
                logger.info(f"Left Tank starting level: {left_tank_level:.2f} cm")
                logger.info(f"Right Tank starting level: {right_tank_level:.2f} cm")
                
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
            logger.info(f"Refill completed for both tanks at {current_time.strftime('%H:%M:%S')}")
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
        
        # Log status occasionally to avoid cluttering the output
        if int(elapsed_minutes) % 10 == 0 and int(elapsed_minutes) > 0:
            logger.info(f"Both tanks refilling: {int(elapsed_minutes)}/{REFILL_DURATION} minutes")
            logger.info(f"Left Tank level: {left_tank_level:.2f} cm")
            logger.info(f"Right Tank level: {right_tank_level:.2f} cm")
        
        return True

def on_connect(client, userdata, flags, reason_code, properties):
    global status_data
    if reason_code == 0:
        logger.info("Connected to MQTT Broker!")
        status_data["mqtt_connected"] = True
        # Publish online status
        try:
            client.publish(lwt_topic, "Online", qos=1, retain=True)
        except Exception as e:
            logger.error(f"Failed to publish online status: {str(e)}")
    else:
        logger.error(f"Failed to connect, return code {reason_code}")
        status_data["mqtt_connected"] = False
        status_data["error"] = f"MQTT connection failed with code {reason_code}"
        # The client will auto-reconnect by default

def on_disconnect(client, userdata, rc, properties):
    global status_data
    status_data["mqtt_connected"] = False
    logger.warning(f"Disconnected with result code {rc}")
    if rc != 0:
        logger.info("Unexpected disconnect. Reconnection will be handled automatically.")

def on_publish(client, userdata, mid, properties):
    # Optionally track successful publishes
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="Water Tank Monitoring Simulation")
    parser.add_argument("--start-time", type=str, default=None,
                       help="Starting simulation time in UTC (YYYY-MM-DD HH:MM:SS). If not provided, current time will be used.")
    return parser.parse_args()

def main():
    global simulation_time, left_tank_level, right_tank_level, status_data
    
    # Update status
    status_data["status"] = "initializing"
    
    # Parse command line arguments
    args = parse_args()
    
    # Set simulation time
    if args.start_time is not None:
        try:
            utc_time = datetime.datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
            simulation_time = pytz.utc.localize(utc_time).astimezone(IST)
            logger.info(f"Starting simulation at: {simulation_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
        except ValueError:
            logger.warning(f"Invalid time format. Using current time.")
            simulation_time = datetime.datetime.now(IST)
    else:
        # Use current time in IST
        simulation_time = datetime.datetime.now(IST)
        logger.info(f"Starting simulation at current time: {simulation_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
    
    # Start web server in a separate thread
    web_thread = threading.Thread(target=start_web_server, daemon=True)
    web_thread.start()
    logger.info("Web server thread started")
    
    # Set up MQTT client with VERSION2 API and random client ID to avoid connection conflicts
    client_id = f"ESP32_TankMonitor_{random.randint(1000, 9999)}"
    client = mqtt.Client(client_id=client_id, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Use system certificates
    client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)

    # Last Will and Testament (LWT)
    client.will_set(lwt_topic, lwt_message, qos=1, retain=True)

    # Set callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # Connect to MQTT broker with retry logic
    connected = False
    retry_count = 0
    max_retries = 10
    
    while not connected and retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to MQTT broker (attempt {retry_count+1})")
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            connected = True
            status_data["mqtt_connected"] = True
            logger.info(f"Successfully connected to MQTT broker with client ID: {client_id}")
        except Exception as e:
            retry_count += 1
            status_data["error"] = f"MQTT connection attempt {retry_count} failed: {str(e)}"
            logger.error(f"Connection attempt {retry_count} failed: {str(e)}")
            if retry_count < max_retries:
                wait_time = min(30, 2 ** retry_count)  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
    
    if not connected:
        status_data["status"] = "error"
        status_data["error"] = "Failed to connect to MQTT broker after maximum retries"
        logger.critical("Failed to connect after maximum retries. Will continue running for web interface.")
    else:
        status_data["status"] = "connected"

    # Start the client loop in the background
    client.loop_start()
    
    # Initialize usage variations for all periods
    update_all_usage_variations()
    
    # Start simulation
    try:
        logger.info("Starting realistic tank monitoring simulation")
        logger.info(f"Current time: {simulation_time.strftime('%Y-%m-%d %H:%M:%S')} {'Weekday' if is_weekday(simulation_time) else 'Weekend'}")
        
        # Show initial usage variations for all periods
        logger.info("Initial usage variations:")
        for period, genders in current_usage_variations.items():
            logger.info(f"  {period}: Girls {genders['girls']:.2f}%, Boys {genders['boys']:.2f}%")
        
        last_day = simulation_time.day
        update_counter = 0
        health_check_counter = 0
        
        # Update status
        status_data["status"] = "running"
        
        while True:
            try:
                # Advance simulation time or use current time
                simulation_time = datetime.datetime.now(IST)
                update_counter += 1
                health_check_counter += 1
                
                # Periodic health check logging (every hour)
                if health_check_counter >= 20:  # 20 x 3 minutes = 1 hour
                    logger.info("Health check: Tank monitoring simulation running normally")
                    health_check_counter = 0
                
                # Check if we've moved to a new day
                if simulation_time.day != last_day:
                    last_day = simulation_time.day
                    logger.info(f"New day: {simulation_time.strftime('%Y-%m-%d')}")
                
                # Get current break period
                current_break = get_current_break_period(simulation_time)
                
                # Update all usage variations every 3 minutes
                update_all_usage_variations()
                
                # Log significant variation changes every few updates to avoid cluttering
                if update_counter % 5 == 0:
                    logger.info("Updated usage variations:")
                    for period, genders in current_usage_variations.items():
                        logger.info(f"  {period}: Girls {genders['girls']:.2f}%, Boys {genders['boys']:.2f}%")
                
                # Handle shared refill for both tanks
                is_refilling = check_and_handle_refill(simulation_time)
                
                if not is_refilling:
                    # If not refilling, calculate consumption for both tanks
                    try:
                        left_consumption = calculate_consumption("girls", current_break, simulation_time)
                        right_consumption = calculate_consumption("boys", current_break, simulation_time)
                        
                        # Update tank levels
                        left_tank_level = max(min(left_tank_level - left_consumption, MAX_LEVEL_CM), MIN_LEVEL_CM)
                        right_tank_level = max(min(right_tank_level - right_consumption, MAX_LEVEL_CM), MIN_LEVEL_CM)
                    except Exception as e:
                        logger.error(f"Error calculating consumption: {str(e)}")
                        status_data["error"] = f"Calculation error: {str(e)}"
                        # Continue with existing levels if there's an error
                
                # Calculate volumes
                left_volume = calculate_volume(left_tank_level)
                right_volume = calculate_volume(right_tank_level)
                
                # Format timestamp to match exact ISO format
                timestamp = simulation_time.isoformat()
                
                # Update status data for web interface
                status_data["last_update"] = timestamp
                
                # Check if client is connected before publishing
                if connected and not client.is_connected():
                    logger.warning("MQTT client disconnected. Attempting to reconnect...")
                    status_data["mqtt_connected"] = False
                    try:
                        client.reconnect()
                        status_data["mqtt_connected"] = True
                    except Exception as e:
                        logger.error(f"Failed to reconnect: {str(e)}")
                        status_data["error"] = f"MQTT reconnection failed: {str(e)}"
                        # Continue and try again next loop
                
                # Publish data to MQTT topics with error handling
                if connected:
                    try:
                        # Publish rounded values to reduce data size
                        client.publish(topic1, round(left_tank_level, 2))
                        client.publish(topic2, round(left_volume, 2))
                        client.publish(topic3, round(right_tank_level, 2))
                        client.publish(topic4, round(right_volume, 2))
                        client.publish(topic5, timestamp)
                        
                        # Status information (only log details every few updates)
                        if update_counter % 10 == 0:
                            logger.info("=" * 40)
                            logger.info(f"Time: {simulation_time.strftime('%Y-%m-%d %H:%M:%S')} {'Weekday' if is_weekday(simulation_time) else 'Weekend'}")
                            logger.info(f"Current period: {current_break}")
                            logger.info(f"Usage %: Girls {current_usage_variations[current_break]['girls']:.2f}%, Boys {current_usage_variations[current_break]['boys']:.2f}%")
                            
                            # Calculate consumption in milliliters for display
                            if not is_refilling:
                                left_consumption_ml = left_consumption * TANK_AREA_CM2 / 1000
                                right_consumption_ml = right_consumption * TANK_AREA_CM2 / 1000
                                logger.info(f"Left Tank (Girls) - Level: {left_tank_level:.2f} cm, Volume: {left_volume:.2f} L, Last change: {left_consumption_ml:.0f} ml")
                                logger.info(f"Right Tank (Boys) - Level: {right_tank_level:.2f} cm, Volume: {right_volume:.2f} L, Last change: {right_consumption_ml:.0f} ml")
                            else:
                                logger.info(f"Left Tank (Girls) - Level: {left_tank_level:.2f} cm, Volume: {left_volume:.2f} L (Refilling)")
                                logger.info(f"Right Tank (Boys) - Level: {right_tank_level:.2f} cm, Volume: {right_volume:.2f} L (Refilling)")
                            
                            logger.info("Data Published!")
                            logger.info("=" * 40)
                    except Exception as e:
                        logger.error(f"Failed to publish MQTT messages: {str(e)}")
                        status_data["error"] = f"MQTT publish error: {str(e)}"
                
                # Sleep for 3 minutes (180 seconds) between updates
                time.sleep(180)
                
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                status_data["error"] = f"Main loop error: {str(e)}"
                # Continue the loop instead of crashing
                time.sleep(10)  # Brief pause before continuing

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Stopping the simulation...")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        status_data["status"] = "error"
        status_data["error"] = f"Fatal error: {str(e)}"
    finally:
        # Always clean up properly
        try:
            # Publish online status before disconnecting
            if connected:
                client.publish(lwt_topic, "Manually Disconnected", qos=1, retain=True)
                client.disconnect()
                client.loop_stop()
            logger.info("Simulation stopped and cleaned up.")
            status_data["status"] = "stopped"
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    main()