import src.e2ap_xapp as e2ap_xapp
from time import sleep
import csv
from ricxappframe.e2ap.asn1 import IndicationMsg
import sys
from datetime import datetime

# Append path to RAN protobuf definitions
sys.path.append("oai-oran-protolib/builds/")
from ran_messages_pb2 import *
 

# Function to initialize the CSV file
def initialize_csv(file_name='gnb_data.csv'):
    with open(file_name, mode='w', newline='') as csv_file:
        fieldnames = ['TIMESTAMP', 'RNTI', 'RSRP', 'UL_BER', 'DL_BER', 'UL_MCS', 'DL_MCS', 'USED_PRBS']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()  # Write the header to the CSV


# Function to write a row of UE data to the CSV file
def write_to_csv(ue_data, file_name='gnb_data.csv'):
    with open(file_name, mode='a', newline='') as csv_file:
        fieldnames = ['TIMESTAMP', 'RNTI', 'RSRP', 'UL_BER', 'DL_BER', 'UL_MCS', 'DL_MCS', 'USED_PRBS']        
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writerow(ue_data)


def process_ue_info(ue_info):
    """Extracts and formats UE info data."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Timestamp with millisecond precision
    rnti = ue_info.rnti
    rsrp = ue_info.rsrp if ue_info.HasField('rsrp') else None

    # Format BER values to six decimal places
    ul_ber = f"{ue_info.ber_uplink:.6f}" if ue_info.HasField('ber_uplink') else None
    dl_ber = f"{ue_info.ber_downlink:.6f}" if ue_info.HasField('ber_downlink') else None

    ul_mcs = ue_info.mcs_uplink if ue_info.HasField('mcs_uplink') else None
    dl_mcs = ue_info.mcs_downlink if ue_info.HasField('mcs_downlink') else None
    used_prbs = ue_info.cell_load if ue_info.HasField('cell_load') else None

    # Return the formatted UE data
    return {
        'TIMESTAMP': current_time,
        'RNTI': rnti,
        'RSRP': rsrp,
        'UL_BER': ul_ber,
        'DL_BER': dl_ber,
        'UL_MCS': ul_mcs,
        'DL_MCS': dl_mcs,
        'USED_PRBS': used_prbs
    }


def xappLogic():
    # Initialize the xApp connector
    connector = e2ap_xapp.e2apXapp()

    # Get gNBs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print(f"{len(gnb_id_list)} gNB connected to RIC, listing:")
    for gnb_id in gnb_id_list:
        print(gnb_id)
    print("---------")

    # Send subscription requests for each gNB
    for gnb in gnb_id_list:
        e2sm_buffer = e2sm_report_request_buffer()
        #connector.send_e2ap_sub_request(e2sm_buffer, gnb)

    # Initialize the CSV file with headers
    initialize_csv()

    # Infinite loop to continuously read messages
    sleep_time = 0.5
    while True:
        print(f"Sleeping {sleep_time}s...")
        sleep(sleep_time)
        messgs = connector.get_queued_rx_message()

        if len(messgs) == 0:
            print("No messages received while waiting.")
        else:
            print(f"{len(messgs)} messages received while waiting, processing:")
            for msg in messgs:
                if msg["message type"] == connector.RIC_IND_RMR_ID:
                    print(f"RIC Indication received from gNB {msg['meid']}, decoding E2SM payload")

                    # Decode the indication message
                    indm = IndicationMsg()
                    indm.decode(msg["payload"])

                    # Parse the RAN indication response
                    resp = RAN_indication_response()
                    resp.ParseFromString(indm.indication_message)

                    # Extract the UE data from the RAN indication response
                    for param_map_entry in resp.param_map:
                        if param_map_entry.key == RAN_parameter.UE_LIST:
                            ue_list = param_map_entry.ue_list

                            # For each UE in the list, extract the required data and write to the CSV
                            for ue in ue_list.ue_info:
                                ue_data = process_ue_info(ue)
                                write_to_csv(ue_data)
                                print(f"Data written for UE RNTI {ue.rnti}")
                    print("Processing complete.")
                else:
                    print(f"Unrecognized E2AP message received from gNB {msg['meid']}")
        connector.send_e2ap_sub_request(e2sm_buffer, gnb)


def e2sm_report_request_buffer():
    # Prepare an E2SM report request to monitor gNB and UE data
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.INDICATION_REQUEST
    inner_mess = RAN_indication_request()

    # We are interested in GNB_ID and UE_LIST parameters
    inner_mess.target_params.extend([RAN_parameter.GNB_ID, RAN_parameter.UE_LIST])
    master_mess.ran_indication_request.CopyFrom(inner_mess)

    # Serialize the request message to send it
    buf = master_mess.SerializeToString()
    return buf


if __name__ == "__main__":
    xappLogic()
