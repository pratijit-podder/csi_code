# udp_5g_csi_plot
This repository is about how to transfer CSI (Channel State Information) and TA (Timing Advance) offset from a SRSRAN physical layer implementation to a host, and for real-time plotting of the received data. And the protocol is UDP.
## Files
- srs_estimator_generic_impl.cpp
  
  Handling physical layer to send CSI and TA offset to another host, using UDP protocol by minor change, please see the comment on line 119-120 for details.
  
  Before starting this project, go to /home/inss/srs_scripts/my_config and change IP and port to your own PC.
- csiplot_udp.py
  
  Receiver script that collects transmitted CSI and TA offset in real time and saves data.
- plot_csi_file.py

  Using saved data to draw graphs again.
## Usage of csiplot_udp.py
1. Set up destination address to match the receiver host in SRSRAN side.
2. Run the receiver.
   ```bash
   python3 csiplot_udp.py
## Usage of plot_csi_file.py
1. Set the file name on line 23.
2. Run the script.
   ```bash
   python3 plot_csi_file.py
