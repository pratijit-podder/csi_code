import numpy as np
import os
import csv

input_folder = "csi_data_logs"
output_folder = "csi_csv_output"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".npz"):
        filepath = os.path.join(input_folder, filename)
        print(f"Processing {filename}...")

        data = np.load(filepath, allow_pickle=True)
        rx_port = data['rx_port']
        tx_port = data['tx_port']
        ta_us = data['ta_us']
        csi = data['csi']

        # Create a subfolder for each file’s data
        base_name = os.path.splitext(filename)[0]
        file_output_folder = os.path.join(output_folder, base_name)
        os.makedirs(file_output_folder, exist_ok=True)

        # Save RX/TX/TA arrays
        np.savetxt(os.path.join(file_output_folder, "rx_port.csv"), rx_port, delimiter=",", fmt="%s")
        np.savetxt(os.path.join(file_output_folder, "tx_port.csv"), tx_port, delimiter=",", fmt="%s")
        np.savetxt(os.path.join(file_output_folder, "ta_us.csv"), ta_us, delimiter=",", fmt="%s")

        # Save CSI array carefully (it's likely multi-dimensional and complex)
        with open(os.path.join(file_output_folder, "csi.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            for row in csi:
                # Flatten and split real/imag parts for readability
                flat_row = []
                for val in row:
                    flat_row.append(f"{val.real:.6f}+{val.imag:.6f}j")
                writer.writerow(flat_row)

        print(f"Saved output to {file_output_folder}")
