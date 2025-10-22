import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading
from collections import deque
import time
import os

# --- Global Data Structures ---
data_lock = threading.Lock()
latest_csi_data = {}
ta_history = deque()
rx_ports = set()

def load_csi_from_file(npz_file):
    data = np.load(npz_file, allow_pickle=True)
    rx_port_arr = data['rx_port']
    tx_port_arr = data['tx_port']
    ta_us_arr = data['ta_us']
    csi_arr = data['csi']
    return rx_port_arr, tx_port_arr, ta_us_arr, csi_arr

def simulate_data_feed_multiple(files, folder="csi_data_logs", feed_interval=0.05, loop=False):
    """
    Simulate real-time data feeding by reading pre-recorded CSI data files.
    """
    global latest_csi_data, ta_history, rx_ports

    while True:
        for filename in files:
            filepath = os.path.join(folder, filename)
            rx_port_arr, tx_port_arr, ta_us_arr, csi_arr = load_csi_from_file(filepath)
            print(f"Playing file {filename}, {len(rx_port_arr)} samples")

            for i in range(len(rx_port_arr)):
                rx_port = int(rx_port_arr[i])
                ta_us = ta_us_arr[i]
                csi = csi_arr[i]

                with data_lock:
                    latest_csi_data[rx_port] = csi
                    ta_history.append(ta_us)
                    rx_ports.add(rx_port)

                time.sleep(feed_interval)

        if not loop:
            print("Finished playing all files.")
            break

# --- Plotting related globals ---
fig = None
axs = {}
lines = {}

def setup_plots():
    global fig, axs, lines

    # Wait until at least one RX port is available
    while not rx_ports:
        time.sleep(0.1)

    n_rx = len(rx_ports)
    rx_port_list = sorted(list(rx_ports))

    fig = plt.figure(figsize=(4 * (n_rx + 1), 8))
    gs = fig.add_gridspec(2, n_rx + 1)

    for i, rx_port in enumerate(rx_port_list):
        ax_mag = fig.add_subplot(gs[0, i])
        l1, = ax_mag.plot([], [], label=f'RX {rx_port}')
        ax_mag.set_title(f"RX {rx_port} - Magnitude", fontsize=14)
        ax_mag.set_ylabel('Magnitude', fontsize=12)
        ax_mag.set_xlabel('Subcarrier Index', fontsize=12)
        ax_mag.grid(True)
        ax_mag.set_ylim(0, 1)

        ax_phase = fig.add_subplot(gs[1, i])
        l2, = ax_phase.plot([], [], label=f'RX {rx_port}')
        ax_phase.set_title(f"RX {rx_port} - Phase", fontsize=14)
        ax_phase.set_ylabel('Unwrapped Phase (rad)', fontsize=12)
        ax_phase.set_xlabel('Subcarrier Index', fontsize=12)
        ax_phase.grid(True)
        ax_phase.set_ylim(-10, 10)

        axs[rx_port] = {'mag': ax_mag, 'phase': ax_phase}
        lines[rx_port] = {'mag': l1, 'phase': l2}

    ax_ta = fig.add_subplot(gs[:, n_rx])
    l_ta, = ax_ta.plot([], [], 'r.-', label='Time Alignment')
    ax_ta.set_title("Time Alignment History", fontsize=14)
    ax_ta.set_ylabel("Time Alignment (µs)", fontsize=12)
    ax_ta.set_xlabel("Sample Index", fontsize=12)
    ax_ta.grid(True)
    ax_ta.legend()

    axs['ta'] = ax_ta
    lines['ta'] = l_ta

    plt.tight_layout(pad=2.0)

def update_plots(frame):
    global latest_csi_data, ta_history, lines, axs

    updated_lines = []

    with data_lock:
        local_csi_data = latest_csi_data.copy()
        local_ta_history = list(ta_history)

    if not local_csi_data or not lines:
        return []

    for rx_port, csi_data in local_csi_data.items():
        if rx_port not in lines:
            continue

        mag = np.abs(csi_data)
        phase_unwrapped = np.unwrap(np.angle(csi_data))
        x_data = np.arange(len(mag))

        lines[rx_port]['mag'].set_data(x_data, mag)
        axs[rx_port]['mag'].set_xlim(0, len(mag) - 1 if len(mag) > 1 else 1)

        lines[rx_port]['phase'].set_data(x_data, phase_unwrapped)
        axs[rx_port]['phase'].set_xlim(0, len(phase_unwrapped) - 1 if len(phase_unwrapped) > 1 else 1)

        updated_lines.extend([lines[rx_port]['mag'], lines[rx_port]['phase']])

    if 'ta' in lines and local_ta_history:
        ta_line = lines['ta']
        ta_ax = axs['ta']

        x_ta_data = np.arange(len(local_ta_history))
        ta_line.set_data(x_ta_data, local_ta_history)

        ta_ax.relim()
        ta_ax.autoscale_view(True, True, True)

        updated_lines.append(ta_line)

    return updated_lines

if __name__ == "__main__":
    folder = "csi_data_logs"
    files = sorted(f for f in os.listdir(folder) if f.endswith('.npz'))

    if not files:
        print("No data files found in folder:", folder)
        exit(1)

    # Load the first file to initialize rx_ports for plot setup
    first_file = os.path.join(folder, files[0])
    rx_port_arr, _, _, _ = load_csi_from_file(first_file)

    with data_lock:
        rx_ports.update(set(rx_port_arr.tolist()))

    # Start a thread to simulate real-time feeding of data from all files
    t = threading.Thread(target=simulate_data_feed_multiple, args=(files, folder, 0.05, False), daemon=True)
    t.start()

    setup_plots()
    ani = FuncAnimation(fig, update_plots, interval=50, blit=False)
    plt.show()
