import asyncio
import json
from quixstreams import Application
from datetime import datetime, timezone
import subprocess
import os
import shutil
import threading
import logging
import colorlog

LOG_FORMAT = '[+] %(log_color)s%(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

logger = colorlog.getLogger()
handler = logging.StreamHandler()

formatter = colorlog.ColoredFormatter(
    LOG_FORMAT, datefmt=DATE_FORMAT,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_nprobe_path():
    exe_name = "nprobe.exe"  
    exe_path = shutil.which(exe_name)
    return exe_path if exe_path else None

def run_nprobe(interface="Intel(R) Wi-Fi 6 AX201 160MHz" , tcp_socket="192.168.100.4" , port=9070):
    nprobe = get_nprobe_path()

    if not nprobe :
        logger.error("nprobe not found in system path.")
        return
    command = f'"{nprobe}" /c -i "{interface}" --tcp {tcp_socket}:{port} -n none -T "%IN_SRC_MAC %OUT_DST_MAC %IPV4_SRC_ADDR %IPV4_DST_ADDR %L4_SRC_PORT %L4_DST_PORT %IPV6_SRC_ADDR %IPV6_DST_ADDR %IP_PROTOCOL_VERSION %PROTOCOL %IN_BYTES %IN_PKTS %OUT_BYTES %OUT_PKTS %TCP_FLAGS %DIRECTION %FLOW_START_SEC %FLOW_END_SEC %FLOW_DURATION_MILLISECONDS %SRC_TO_DST_IAT_MIN %SRC_TO_DST_IAT_MAX %SRC_TO_DST_IAT_AVG %SRC_TO_DST_IAT_STDDEV %DST_TO_SRC_IAT_MIN %DST_TO_SRC_IAT_MAX %DST_TO_SRC_IAT_AVG %DST_TO_SRC_IAT_STDDEV %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN" '
    logger.info(command)

    log_directory='logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    stdout_log_file = os.path.join(log_directory, f'output_{timestamp}.log')
    stderr_log_file = os.path.join(log_directory, f'error_{timestamp}.log')

    try:
        logger.info("Running nprobe command...")
        result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)

        output = result.stdout
        error = result.stderr

        with open(stdout_log_file, 'a') as stdout_file:
            stdout_file.write(output)
        with open(stderr_log_file, 'a') as stderr_file:
            stderr_file.write(error)

        logger.info("nprobe command executed successfully.")

    except subprocess.CalledProcessError as e:
        with open(stderr_log_file, 'a') as stderr_file:
            stderr_file.write(e.stderr)

        logger.error(f"Error while running nprobe command check logs directory ")

HOST = '192.168.100.4'
PORT = 9070

app = Application(broker_address="192.168.100.4:9093", loglevel="DEBUG")

def transform_data(json_data):

    if 'FLOW_START_SEC' in json_data:
        json_data['FLOW_START_TIME'] = datetime.fromtimestamp(json_data['FLOW_START_SEC'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        del json_data['FLOW_START_SEC']  

    if 'FLOW_END_SEC' in json_data:
        json_data['FLOW_END_TIME'] = datetime.fromtimestamp(json_data['FLOW_END_SEC'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        del json_data['FLOW_END_SEC'] 

    if 'FLOW_DURATION_MILLISECONDS' in json_data:
        json_data['FLOW_DURATION_SECONDS'] = json_data['FLOW_DURATION_MILLISECONDS'] / 1000
        del json_data['FLOW_DURATION_MILLISECONDS']  

    if 'DIRECTION' in json_data:
        json_data['DIRECTION'] = 'RX' if json_data['DIRECTION'] == 0 else 'TX'

    return json_data

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logger.info("Handling new client connection...")

    producer = app.get_producer()
    
    try:
        while True:
            line = await reader.readline()  
            if not line:
                logger.info("Client disconnected.")
                break  

            try:
                json_data = json.loads(line.decode('utf-8').strip()) 
                flow_data = transform_data(json_data)

                producer.produce(topic="netflow_topic", key="netflow", value=json.dumps(flow_data))
                await asyncio.sleep(0)

            except json.JSONDecodeError:
                print("Invalid JSON received, ignoring...")

    except asyncio.CancelledError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = server.sockets[0].getsockname()
    logger.info(f"Starting the server on {addr}")

    nprobe_thread = threading.Thread(target=run_nprobe, args=("Intel(R) Wi-Fi 6 AX201 160MHz", "192.168.100.4", 9070))
    nprobe_thread.start()

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())



# nprobe.exe /c -i "Intel(R) Wi-Fi 6 AX201 160MHz" --tcp 192.168.100.4:9070 -n none -T "%IN_SRC_MAC %OUT_DST_MAC %IPV4_SRC_ADDR %IPV4_DST_ADDR %L4_SRC_PORT %L4_DST_PORT %IPV6_SRC_ADDR %IPV6_DST_ADDR %IP_PROTOCOL_VERSION %PROTOCOL %IN_BYTES %IN_PKTS %OUT_BYTES %OUT_PKTS %TCP_FLAGS %DIRECTION %FLOW_START_SEC %FLOW_END_SEC %FLOW_DURATION_MILLISECONDS %SRC_TO_DST_IAT_MIN %SRC_TO_DST_IAT_MAX %SRC_TO_DST_IAT_AVG %SRC_TO_DST_IAT_STDDEV %DST_TO_SRC_IAT_MIN %DST_TO_SRC_IAT_MAX %DST_TO_SRC_IAT_AVG %DST_TO_SRC_IAT_STDDEV %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN  "
