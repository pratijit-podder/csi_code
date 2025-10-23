import numpy as np
import os
import pandas as pd

input_folder = "csi_data_logs"
output_folder = "csi_csv_labeled"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".npz"):
        filepath = os.path.join(input_folder, filename)
        print(f"Processing {filename}...")

        # Load CSI data
        data = np.load(filepath, allow_pickle=True)
        csi = data['csi']  # shape: (num_snapshots, num_subcarriers)

        # Convert complex values to string "a+bj" for readability
        csi_str = np.array(
            [ [f"{val.real:.6f}{'+' if val.imag >= 0 else ''}{val.imag:.6f}j" for val in row] for row in csi ]
        )

        # Create column labels
        num_subcarriers = csi.shape[1]
        col_labels = [f"Subcarrier_{i}" for i in range(num_subcarriers)]

        # Create DataFrame with labeled columns and snapshot index
        df = pd.DataFrame(csi_str, columns=col_labels)
        df.insert(0, "Snapshot", np.arange(len(csi)))

        # Save to CSV
        base_name = os.path.splitext(filename)[0]
        output_path = os.path.join(output_folder, f"{base_name}_labeled.csv")
        df.to_csv(output_path, index=False)

        print(f"Labeled CSV saved to: {output_path}")
