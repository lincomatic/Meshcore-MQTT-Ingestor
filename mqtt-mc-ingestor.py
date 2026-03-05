import asyncio
import configparser
import json
import logging
from pathlib import Path
import random
import re
import signal
import ssl
import sys
import aiomqtt

import database


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
logger.info("Script starting, loading configuration...")

# Global database writer instance
db_writer = None
shutdown_event = asyncio.Event()

# Map protocol string to aiomqtt protocol version
protocol_map = {
    "v5": 5,
    "v311": 4,
    "v31": 3,
}

TOPIC_PATTERN = re.compile(
    r"^meshcore/(?P<iata>[A-Z]{3})/(?P<pubkey>[A-Fa-f0-9]{64})/(?P<subtopic>packets|status)$"
)


def parse_meshcore_topic(topic):
    match = TOPIC_PATTERN.match(topic)
    if not match:
        return None
    topic_data = match.groupdict()
    topic_data["pubkey"] = topic_data["pubkey"].upper()
    return topic_data


def parse_json_payload(payload):
    if isinstance(payload, bytes):
        payload_text = payload.decode("utf-8", errors="replace")
    else:
        payload_text = str(payload)
    return json.loads(payload_text)


async def handle_packets_message(remote_name, topic_data, payload):
    """Handle packets message - validate and enqueue to database."""
    try:
        parsed_payload = parse_json_payload(payload)
        
        # Add topic data to payload for database insertion
        parsed_payload['_topic_data'] = topic_data
        
        # Enqueue to database writer
        if db_writer:
            await db_writer.enqueue_packet(parsed_payload)
            logger.debug(
                "[%s] [PACKETS] Enqueued: iata=%s observer=%s",
                remote_name,
                topic_data["iata"],
                topic_data["pubkey"][:4],
            )
    except Exception as e:
        logger.error(
            "[%s] [PACKETS] Error handling packet: %s",
            remote_name,
            e,
            exc_info=True
        )


async def handle_status_message(remote_name, topic_data, payload):
    """Handle status message - enqueue to database."""
    try:
        parsed_payload = parse_json_payload(payload)
        
        # Add topic data to payload for database insertion
        parsed_payload['_topic_data'] = topic_data
        
        # Enqueue to database writer
        if db_writer:
            await db_writer.enqueue_observer(parsed_payload)
            logger.debug(
                "[%s] [STATUS] Enqueued: pubkey=%s name=%s",
                remote_name,
                parsed_payload.get('origin_id', '')[:8],
                parsed_payload.get('origin', ''),
            )
    except Exception as e:
        logger.error(
            "[%s] [STATUS] Error handling status: %s",
            remote_name,
            e,
            exc_info=True
        )

def extract_iatas_from_topics(remotes: list) -> set:
    """
    Extract IATA codes from subscribed topics.
    Topics are of the form meshcore/IATA/#
    
    Args:
        remotes: List of remote configurations
        
    Returns:
        Set of unique IATA codes
    """
    iatas = set()
    for remote in remotes:
        for topic, _ in remote.get("topics", []):
            # Parse topic format: meshcore/IATA/#
            parts = topic.split("/")
            if len(parts) >= 2:
                iata = parts[1].upper()
                if iata != "#":  # Skip wildcard
                    iatas.add(iata)
    return iatas

def load_config():
    parser = configparser.ConfigParser()
    config_path = Path(__file__).with_name("config.ini")
    logger.info("Looking for config file at: %s", config_path.absolute())
    if not config_path.exists():
        logger.error("Config file not found at %s", config_path.absolute())
        raise FileNotFoundError(f"mirror.ini not found at {config_path.absolute()}")
    parser.read(config_path, encoding="utf-8")
    logger.debug("Config file read, sections found: %s", parser.sections())

    global_cfg = parser["global"] if parser.has_section("global") else None
    logger.debug("Parsed [global]section")
    log_level = global_cfg.get("log_level", "INFO") if global_cfg else "INFO"
    test_mode = parser.getboolean("global", "test", fallback=False)
    test_topic = global_cfg.get("test_topic", "test") if global_cfg else "test"
    test_topic = test_topic.strip("\"'")

    remotes = []
    for section_name in parser.sections():
        if not section_name.lower().startswith("remote"):
            continue
        remote = parser[section_name]
        remote_enabled = remote.getboolean("enable", fallback=False)

        # Validate required parameters
        remote_host = remote.get("host")
        if not remote_host:
            raise KeyError(f"[{section_name}] Required parameter 'host' is missing")
        if not remote.get("port"):
            raise KeyError(f"[{section_name}] Required parameter 'port' is missing")
        remote_port = remote.getint("port")

        remote_qos = remote.getint("qos", fallback=0)
        if remote_qos not in (0, 1, 2):
            raise ValueError(f"[{section_name}] Invalid qos={remote_qos}, must be 0, 1, or 2")

        remote_use_tls = remote.getboolean("use_tls", fallback=False)
        remote_tls_insecure = remote.getboolean("tls_insecure", fallback=False)
        remote_use_websockets = remote.getboolean("use_websockets", fallback=False)
        remote_session_expiry = remote.getint("session_expiry", fallback=0)
        remote_retry_interval = remote.getint("retry_interval", fallback=15)
        remote_keepalive = remote.getint("keepalive", fallback=60)

        remote_protocol_str = remote.get("protocol", fallback="v311").lower()
        remote_protocol = protocol_map.get(remote_protocol_str, 4)
        
        remote_topics = [(topic.strip(), remote_qos) for topic in remote.get("topics", "").split(",") if topic.strip()]
        if remote_enabled and not remote_topics:
            raise ValueError(f"[{section_name}] Enabled remote has no topics configured")

        remotes.append(
            {
                "name": section_name,
                "enabled": remote_enabled,
                "host": remote_host,
                "port": remote_port,
                "user": remote.get("user"),
                "password": remote.get("pass"),
                "qos": remote_qos,
                "use_tls": remote_use_tls,
                "tls_insecure": remote_tls_insecure,
                "use_websockets": remote_use_websockets,
                "session_expiry": remote_session_expiry,
                "retry_interval": remote_retry_interval,
                "keepalive": remote_keepalive,
                "protocol": remote_protocol,
                "protocol_str": remote_protocol_str,
                "topics": remote_topics,
            }
        )

    # Validate at least one enabled remote exists
    if not any(r["enabled"] for r in remotes):
        raise ValueError("No enabled [remote*] sections found in mirror.ini. "
                         "At least one remote must have enable=true")

    # Load database settings
    db_cfg = parser["database"] if parser.has_section("database") else None
    db_batch_size = db_cfg.getint("batch_size", fallback=50) if db_cfg else 50
    db_batch_timeout = db_cfg.getfloat("batch_timeout", fallback=30.0) if db_cfg else 30.0

    return {
        "REMOTES": remotes,
        "GLOBAL_LOG_LEVEL": log_level,
        "GLOBAL_TEST": test_mode,
        "GLOBAL_TEST_TOPIC": test_topic,
        "DB_BATCH_SIZE": db_batch_size,
        "DB_BATCH_TIMEOUT": db_batch_timeout,
    }


try:
    settings = load_config()
    logger.info("Config loaded successfully")
    REMOTES = settings["REMOTES"]
    GLOBAL_LOG_LEVEL = settings["GLOBAL_LOG_LEVEL"]
    GLOBAL_TEST = settings["GLOBAL_TEST"]
    GLOBAL_TEST_TOPIC = settings["GLOBAL_TEST_TOPIC"]
    DB_BATCH_SIZE = settings["DB_BATCH_SIZE"]
    DB_BATCH_TIMEOUT = settings["DB_BATCH_TIMEOUT"]
    logger.info("Loaded %d remote broker config(s)", len(REMOTES))
    resolved_log_level = getattr(logging, str(GLOBAL_LOG_LEVEL).upper(), logging.INFO)
    logging.getLogger().setLevel(resolved_log_level)
    logger.setLevel(resolved_log_level)
    logger.info("Log level set to: " + str(GLOBAL_LOG_LEVEL).upper())
except Exception as e:
    logger.error("Failed to load configuration: %s", e, exc_info=True)
    raise



async def mirror_remote_broker(remote_cfg):
    """Connect to a remote broker  with automatic reconnection."""
    remote_name = remote_cfg["name"]
    logger.debug("[%s] mirror_remote_broker task started", remote_name)
    
    if not remote_cfg["enabled"]:
        logger.info("[%s] Disabled by config (enable=false), skipping connection", remote_name)
        return

    client_id = f"mqtt{remote_name}{random.randint(1000000, 9999999)}"
    while True:  # Infinite reconnection loop
        try:
            logger.info(
                "[%s] client ID: %s, protocol: %s, use_websockets: %s",
                remote_name,
                client_id,
                remote_cfg['protocol_str'],
                remote_cfg['use_websockets']
            )
            logger.debug("[%s] Connecting to %s:%s", remote_name, remote_cfg['host'], remote_cfg['port'])
            
            # Build TLS parameters if needed (always needed when use_tls=true, even if insecure)
            tls_params = None
            if remote_cfg["use_tls"]:
                tls_params = aiomqtt.TLSParameters(
                    ca_certs=None,
                    certfile=None,
                    keyfile=None,
                    cert_reqs=ssl.CERT_NONE if remote_cfg["tls_insecure"] else ssl.CERT_REQUIRED,
                    ciphers=None
                )

            async with aiomqtt.Client(
                hostname=remote_cfg["host"],
                port=remote_cfg["port"],
                protocol=aiomqtt.ProtocolVersion(remote_cfg["protocol"]),
                username=remote_cfg["user"],
                password=remote_cfg["password"],
                identifier=client_id,
                keepalive=remote_cfg["keepalive"],
                clean_session=(remote_cfg["session_expiry"] == 0),
                transport="websockets" if remote_cfg["use_websockets"] else "tcp",
                tls_params=tls_params,
            ) as client:
                logger.info("[%s] Connected to %s:%s", remote_name, remote_cfg['host'], remote_cfg['port'])
                
                # Subscribe to all configured topics
                for topic, qos in remote_cfg["topics"]:
                    await client.subscribe(topic, qos)
                    logger.info("[%s] Subscribed to %s (qos=%d)", remote_name, topic, qos)
                
                # Listen for messages
                async for message in client.messages:
                    # Convert topic to string if needed
                    msg_topic = str(message.topic)
                    logger.debug(
                        "[%s] Received message on %s (qos=%d, retain=%s, payload_size=%d bytes)",
                        remote_name,
                        msg_topic,
                        message.qos,
                        message.retain,
                        len(message.payload)
                    )
                    topic_data = parse_meshcore_topic(msg_topic)
                    if not topic_data:
# ignore non-matching topics such as raw
#                        logger.warning("[%s] Ignoring unexpected topic format: %s", remote_name, msg_topic)
                        continue

                    try:
                        if topic_data["subtopic"] == "packets":
                            await handle_packets_message(remote_name, topic_data, message.payload)
                        elif topic_data["subtopic"] == "status":
                            await handle_status_message(remote_name, topic_data, message.payload)
                    except json.JSONDecodeError as e:
                        logger.error("[%s] Invalid JSON payload on %s: %s", remote_name, msg_topic, e)
                    
        except aiomqtt.MqttError as e:
            logger.error(
                "[%s] MQTT error: %s. Retrying in %ds...",
                remote_name,
                e,
                remote_cfg['retry_interval'],
                exc_info=True
            )
            await asyncio.sleep(remote_cfg['retry_interval'])
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "[%s] Unexpected error: %s. Retrying in %ds...",
                remote_name,
                e,
                remote_cfg['retry_interval'],
                exc_info=True
            )
            await asyncio.sleep(remote_cfg['retry_interval'])


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info("Received signal %d, initiating shutdown...", signum)
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info("Signal handlers registered")


async def main():
    """Main coroutine that manages local broker connection and remote mirror tasks."""
    global db_writer
    
    logger.debug("[CONFIG] Remotes loaded: %d", len(REMOTES))
    for remote in REMOTES:
        logger.debug(
            "[CONFIG] Remote '%s': enabled=%s, host=%s, port=%s",
            remote['name'],
            remote['enabled'],
            remote['host'],
            remote['port']
        )
    
    logger.info(
        "[CONFIG] global.log_level=%s test=%s test_topic=%s",
        str(GLOBAL_LOG_LEVEL).upper(),
        str(GLOBAL_TEST).lower(),
        GLOBAL_TEST_TOPIC
    )
    logger.info("[CONFIG] Loaded remote topic subscriptions:")
    for remote in REMOTES:
        logger.info(
            "[%s] enable=%s protocol=%s use_tls=%s tls_insecure=%s use_websockets=%s session_expiry=%d retry_interval=%d keepalive=%d",
            remote['name'],
            str(remote['enabled']).lower(),
            remote['protocol_str'],
            str(remote['use_tls']).lower(),
            str(remote['tls_insecure']).lower(),
            str(remote['use_websockets']).lower(),
            remote['session_expiry'],
            remote['retry_interval'],
            remote['keepalive']
        )
        if remote["enabled"]:
            logger.info("[%s] Topics:", remote['name'])
            for topic, qos in remote["topics"]:
                logger.info("[%s] - %s (qos=%d)", remote['name'], topic, qos)
        else:
            logger.info("[%s] Skipping topics because broker is disabled", remote['name'])

    enabled_remotes = [remote for remote in REMOTES if remote["enabled"]]
    if not enabled_remotes:
        logger.warning("No enabled remotes found; nothing to run")
        return

    # Initialize and start database writer
    db_path = "meshcore.db"
    logger.info("Initializing database: %s (batch_size=%d, batch_timeout=%.1fs)", db_path, DB_BATCH_SIZE, DB_BATCH_TIMEOUT)
    db_writer = database.DatabaseWriter(db_path=db_path, batch_size=DB_BATCH_SIZE, batch_timeout=DB_BATCH_TIMEOUT)
    await db_writer.start()
    
    # Extract and populate IATA codes from subscribed topics
    iatas = extract_iatas_from_topics(enabled_remotes)
    if iatas:
        logger.info("Populating IATA table with codes: %s", ", ".join(sorted(iatas)))
        await db_writer.populate_iatas(list(iatas))
    else:
        logger.warning("No IATA codes found in topic subscriptions")

    tasks = []
    try:
        # Start MQTT mirror tasks
        for remote in enabled_remotes:
            task = asyncio.create_task(
                mirror_remote_broker(remote),
                name=f"mirror-{remote['name']}"
            )
            tasks.append(task)

        logger.info("Started %d mirror task(s)", len(tasks))
        
        # Wait for shutdown signal or tasks to complete
        done, pending = await asyncio.wait(
            [asyncio.create_task(shutdown_event.wait())] + tasks,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        if shutdown_event.is_set():
            logger.info("Shutdown signal received, stopping tasks...")
        
    finally:
        # Cancel all MQTT tasks
        logger.info("Cancelling MQTT tasks...")
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop database writer (flushes queue)
        logger.info("Stopping database writer...")
        if db_writer:
            await db_writer.stop()
        
        logger.info("Shutdown complete")


if __name__ == "__main__":
    # Setup signal handlers before starting event loop
    setup_signal_handlers()
    
    try:
        # On Windows, use SelectorEventLoop instead of ProactorEventLoop
        # because aiomqtt/paho-mqtt requires add_reader/add_writer support
        if sys.platform == "win32":
            loop = asyncio.SelectorEventLoop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(main())
            finally:
                loop.close()
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Fatal error in main: %s", e, exc_info=True)
        raise
    finally:
        logger.info("Program exited")
