import src.e2ap_xapp as e2ap_xapp
from time import sleep, time
from datetime import datetime
import csv
from ricxappframe.e2ap.asn1 import IndicationMsg

import sys
sys.path.append("oai-oran-protolib/builds/")
from ran_messages_pb2 import *

CSV_FILE_PATH = "ran_data.csv"

def send_indication_request(connector, gnb):
    """
    Sends an Indication Request message to the gNB.
    """
    e2sm_buffer = e2sm_report_request_buffer()
    connector._rmr_send_w_meid(e2sm_buffer, connector.RIC_IND_RMR_ID, bytes(gnb, 'ascii'))


def wait_for_response(connector, timeout=1):
    """
    Waits for an Indication Response message from the gNB.
    
    Parameters:
        connector: The xApp connector instance.
        timeout: The maximum wait time for a response, in seconds.

    Returns:
        The response message if received, otherwise None.
    """
    start_time = time()
    while time() - start_time < timeout:
        messgs = connector.get_queued_rx_message()
        if len(messgs) > 0:
            for msg in messgs:
                if msg["message type"] == connector.RIC_IND_RMR_ID:
                    return msg  # Return the message if it's the expected response
        sleep(0.1)  # Sleep briefly before polling again

    return None  # Return None if no message was received within the timeout period


def process_response(msg):
    """
    Process the Indication Response message received from the gNB.
    
    Parameters:
        msg: The response message.
    """
    print("RIC Indication received from gNB {}, decoding E2SM payload".format(msg["meid"]))
    indm = IndicationMsg()
    indm.decode(msg["payload"])
    resp = RAN_indication_response()
    resp.ParseFromString(indm.indication_message)
    
    # Extract the UE information and save it to the CSV
    ue_data_list = extract_ue_data(resp)
    for ue_data in ue_data_list:
        save_to_csv(ue_data)


def extract_ue_data(resp):
    """
    Extracts the UE data from the Indication Response message.

    Parameters:
        resp: The parsed Indication Response message.

    Returns:
        A list of dictionaries containing UE data fields.
    """
    ue_data_list = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # millisecond accuracy
    for entry in resp.param_map:
        if entry.key == RAN_parameter.UE_LIST:
            for ue_info in entry.value.ue_list.ue_info:
                ue_data = {
                    "timestamp": timestamp,
                    "rnti": ue_info.rnti,
                    "rsrp": ue_info.rsrp,
                    "ul_ber": f"{ue_info.ber_uplink:.6f}" if ue_info.HasField('ber_uplink') else None,
                    "dl_ber": f"{ue_info.ber_downlink:.6f}" if ue_info.HasField('ber_downlink') else None,
                    "ul_mcs": ue_info.mcs_uplink if ue_info.HasField('mcs_uplink') else None,
                    "dl_mcs": ue_info.mcs_downlink if ue_info.HasField('mcs_downlink') else None,
                    "used_prbs": ue_info.cell_load if ue_info.HasField('cell_load') else None
                }
                ue_data_list.append(ue_data)
    return ue_data_list


def save_to_csv(ue_data):
    """
    Save the UE data to a CSV file.
    
    Parameters:
        ue_data: A dictionary containing UE data fields.
    """
    file_exists = False
    try:
        file_exists = open(CSV_FILE_PATH)
    except:
        pass
    fieldnames = ["timestamp", "rnti", "rsrp", "ul_ber", "dl_ber", "ul_mcs", "dl_mcs", "used_prbs"]

    with open(CSV_FILE_PATH, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()  # Write headers only if the file doesn't exist
        writer.writerow(ue_data)


def xappLogic():
    # Instantiate xapp 
    connector = e2ap_xapp.e2apXapp()

    # Get gNBs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print("{} gNB(s) connected to RIC, listing:".format(len(gnb_id_list)))
    for gnb_id in gnb_id_list:
        print(gnb_id)
    print("---------")

    if not gnb_id_list:
        print("No gNB connected. Exiting.")
        return

    # Read loop
    sleep_time = 0.5  # 0.5 seconds between sending requests
    while True:
        print(f"Sending Indication Request to gNB(s) and waiting for response...")

        for gnb in gnb_id_list:
            # Send Indication Request
            send_indication_request(connector, gnb)

            # Wait for Indication Response
            response_msg = wait_for_response(connector, timeout=1)  # Timeout of 1 second
            if response_msg:
                # Process the response if received
                process_response(response_msg)
            else:
                print(f"No response received from gNB {gnb} within the timeout period.")

        # Sleep before the next iteration
        sleep(sleep_time)


def e2sm_report_request_buffer():
    """
    Creates and serializes an Indication Request (RAN_message).
    """
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.INDICATION_REQUEST
    inner_mess = RAN_indication_request()
    inner_mess.target_params.extend([RAN_parameter.GNB_ID, RAN_parameter.UE_LIST])
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    return buf


if __name__ == "__main__":
    xappLogic()
