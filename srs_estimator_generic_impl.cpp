/*
 *
 * Copyright 2021-2025 Software Radio Systems Limited
 *
 * This file is part of srsRAN.
 *
 * srsRAN is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * srsRAN is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * A copy of the GNU Affero General Public License can be found in
 * the LICENSE file in the top-level directory of this distribution
 * and at http://www.gnu.org/licenses/.
 *
 */

#include "srs_estimator_generic_impl.h"
#include "srs_validator_generic_impl.h"
#include "srsran/adt/complex.h"
#include "srsran/adt/expected.h"
#include "srsran/adt/static_vector.h"
#include "srsran/adt/tensor.h"
#include "srsran/phy/support/resource_grid_reader.h"
#include "srsran/phy/upper/signal_processors/srs/srs_estimator_configuration.h"
#include "srsran/phy/upper/signal_processors/srs/srs_estimator_result.h"
#include "srsran/ran/cyclic_prefix.h"
#include "srsran/ran/srs/srs_constants.h"
#include "srsran/ran/srs/srs_information.h"
#include "srsran/srsvec/add.h"
#include "srsran/srsvec/dot_prod.h"
#include "srsran/srsvec/mean.h"
#include "srsran/srsvec/prod.h"
#include "srsran/srsvec/sc_prod.h"
#include "srsran/srsvec/subtract.h"

using namespace srsran;

/**************************************************************************************************/
// SCTP Multi-threading and File I/O Section
/**************************************************************************************************/

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <string>
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <atomic>
#include <memory>

#ifdef IPPROTO_SCTP
#undef IPPROTO_SCTP
#endif
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <pthread.h>

#include <chrono>
#include <ctime>

// Data structure to hold CSI data for the queue
struct CsiDataPacket {
  std::vector<cf_t> csi;
  unsigned          rx_port;
  unsigned          tx_port;
  double            time_alignment;
};

class SctpTransmitter {
public:
  SctpTransmitter(const char* server_ip_, int port_) : server_ip(server_ip_), port(port_), stop_flag(false)
  {
    worker_thread = std::thread(&SctpTransmitter::run, this);
  }
  ~SctpTransmitter()
  {
    stop_flag.store(true);
    cv.notify_one();
    if (worker_thread.joinable()) {
      worker_thread.join();
    }
    if (sctp_sock >= 0) {
      close(sctp_sock);
    }
  }
  // Non-blocking function for the main thread to queue data
  void queue_csi_data(srsran::span<const cf_t> csi, unsigned rx_port, unsigned tx_port, double time_alignment)
  {
    std::unique_ptr<CsiDataPacket> packet = std::make_unique<CsiDataPacket>();
    packet->csi.assign(csi.begin(), csi.end());
    packet->rx_port = rx_port;
    packet->tx_port = tx_port;
    packet->time_alignment = time_alignment;
    {
      std::lock_guard<std::mutex> lock(mtx);
      data_queue.push(std::move(packet));
    }
    cv.notify_one();
  }

private:
  struct sockaddr_in servaddr {};
  void init_sctp_sender()
  {
    // It's actually udp_sock, but I don't wan to change to much variable names in case we need to change back to SCTP later.
    // To enable SCTP, uncomment line 121, 132-137 and 160. And comment out 122,161,162.
    // sctp_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    sctp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sctp_sock < 0) {
      perror("SCTP socket creation failed");
      return;
    }

    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);
    inet_pton(AF_INET, server_ip, &servaddr.sin_addr);

    // if (connect(sctp_sock, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
    //   perror("SCTP connect failed");
    //   close(sctp_sock);
    //   sctp_sock = -1;
    //   return;
    // }

    printf("SCTP connected to %s:%d\n", server_ip, port);
  }

  void send_csi_sctp(const CsiDataPacket& packet)
  {
    if (sctp_sock < 0) {
      return;
    }

    std::vector<float> buf;
    buf.reserve(packet.csi.size() * 2 + 3);

    buf.push_back(static_cast<float>(packet.rx_port));
    buf.push_back(static_cast<float>(packet.tx_port));
    buf.push_back(static_cast<float>(packet.time_alignment));

    for (const auto& val : packet.csi) {
      buf.push_back(val.real());
      buf.push_back(val.imag());
    }

    // ssize_t n = send(sctp_sock, buf.data(), buf.size() * sizeof(float), 0);
    ssize_t n = sendto(sctp_sock, buf.data(), buf.size() * sizeof(float), 0,
                   (struct sockaddr*)&servaddr, sizeof(servaddr));
    if (n < 0) {
      perror("send failed");
      // Optional: handle re-connection logic here
    }
  }

  void run()
  {

    pthread_t this_thread = pthread_self();
    pthread_setname_np(this_thread, "sctp_sender"); 

    init_sctp_sender();

    while (!stop_flag.load()) {
      std::unique_ptr<CsiDataPacket> packet_to_send;
      {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return !data_queue.empty() || stop_flag.load(); });

        if (stop_flag.load()) {
          break;
        }

        packet_to_send = std::move(data_queue.front());
        data_queue.pop();
      }

      if (packet_to_send) {
        send_csi_sctp(*packet_to_send);
      }
    }
  }

  const char* server_ip;
  int                                        port;
  int                                        sctp_sock = -1;
  std::thread                                worker_thread;
  std::queue<std::unique_ptr<CsiDataPacket>> data_queue;
  std::mutex                                 mtx;
  std::condition_variable                    cv;
  std::atomic<bool>                          stop_flag;
};

// Global instance of the SCTP transmitter.
// It will be initialized once, starting the background thread.
// Change ip and port at /home/inss/srs_scripts/my_config
std::map<std::string, std::string> load_config_file(const std::string& filename)
{
    std::map<std::string, std::string> result;
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Warning: Cannot open config file: " << filename << std::endl;
        return result;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream is_line(line);
        std::string key;
        if (std::getline(is_line, key, '=')) {
            std::string value;
            if (std::getline(is_line, value)) {
                result[key] = value;
            }
        }
    }
    return result;
}
std::string get_ip_from_config()
{
    auto cfg = load_config_file("/home/inss/srs_scripts/my_config");
    if (cfg.count("ip")) {
        return cfg["ip"];
    }
    return "127.0.0.1";
}

uint16_t get_port_from_config()
{
    auto cfg = load_config_file("/home/inss/srs_scripts/my_config");
    if (cfg.count("port")) {
        return static_cast<uint16_t>(std::stoi(cfg["port"]));
    }
    return 5000;
}
std::string ip = get_ip_from_config();
uint16_t port = get_port_from_config();

SctpTransmitter sctp_transmitter(ip.c_str(), port);

static void dump_csi_bin(srsran::span<cf_t> csi, unsigned rx_port, unsigned tx_port)
{
    const std::string dir = "/home/inss/srs_MIMO_data/";
    std::filesystem::create_directories(dir);
    const std::string fname = dir + "csi_ul_srs_rx" + std::to_string(rx_port)
                            + "_tx" + std::to_string(tx_port) + ".bin";
    std::ofstream fout(fname, std::ios::binary | std::ios::out | std::ios::trunc);
    for (unsigned k = 0; k < csi.size(); ++k) {
        float re = csi[k].real();
        float im = csi[k].imag();
        fout.write(reinterpret_cast<const char*>(&re), sizeof(float));
        fout.write(reinterpret_cast<const char*>(&im), sizeof(float));
    }
    fout.close();
}

static void dump_time_alignment(double ta_seconds)
{
    const std::string dir = "/home/inss/srs_MIMO_data/";
    std::filesystem::create_directories(dir);
    const std::string fname = dir + "time_alignment_log.txt";
    std::ofstream fout(fname, std::ios::out | std::ios::app);
    auto now       = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    fout << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X") << '.' << std::setfill('0') << std::setw(3)
         << ms.count() << ", " << std::fixed << std::setprecision(9) << ta_seconds << "\n";
    fout.close();
}

/**************************************************************************************************/


// /**************************************************************************************************/

// #include <filesystem> 
// #include <fstream>
// #include <iomanip>
// #include <string>
// #include <iostream>

// #ifdef IPPROTO_SCTP
// #undef IPPROTO_SCTP
// #endif
// #include <sys/socket.h>
// #include <netinet/in.h>
// #include <arpa/inet.h>
// #include <unistd.h>

// //#include "sctp_glocals.h"


// int sctp_sock = -1;

// void init_sctp_sender(const char* server_ip, int port)
// {
//     sctp_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
//     if (sctp_sock < 0) {
//         perror("SCTP socket creation failed");
//         exit(1);
//     }

//     struct sockaddr_in servaddr{};
//     servaddr.sin_family = AF_INET;
//     servaddr.sin_port = htons(port);
//     inet_pton(AF_INET, server_ip, &servaddr.sin_addr);

//     if (connect(sctp_sock, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
//         perror("SCTP connect failed");
//         exit(1);
//     }

//     printf("SCTP connected to %s:%d\n", server_ip, port);
// }

// //int sctp_sock = 0;

// static void send_csi_sctp(srsran::span<cf_t> csi, unsigned rx_port, unsigned tx_port)
// {
//     if (sctp_sock < 0) return;

//     std::vector<float> buf;
//     buf.reserve(csi.size() * 2 + 2);

//     buf.push_back((float)rx_port);
//     buf.push_back((float)tx_port);

//     for (unsigned k = 0; k < csi.size(); k++) {
//         buf.push_back(csi[k].real());
//         buf.push_back(csi[k].imag());
//     }

//     ssize_t n = send(sctp_sock, buf.data(), buf.size() * sizeof(float), 0);
//     if (n < 0) {
//         perror("send failed");
//     }
// }

// /*
// static void dump_csi_txt(srsran::span<cf_t> csi, unsigned rx_port, unsigned tx_port)
// {   
//     std::ofstream fout("/home/inss/srs_MIMO_data/csi_subcarrier.txt", std::ios::app);
//     fout << "RX " << rx_port << " TX " << tx_port << "\n";

//     for (unsigned k = 0; k < csi.size(); k++) {
//         fout << csi[k].real() << "," << csi[k].imag();
//         if (k + 1 != csi.size()) fout << " ";
//     }
//     fout << "\n";
// }*/

// static void dump_csi_bin(srsran::span<cf_t> csi, unsigned rx_port, unsigned tx_port)
// {
//     const std::string dir = "/home/inss/srs_MIMO_data/";
//     std::filesystem::create_directories(dir);

//     const std::string fname = dir + "csi_ul_srs_rx" + std::to_string(rx_port)
//                             + "_tx" + std::to_string(tx_port) + ".bin";

//     std::ofstream fout(fname, std::ios::binary | std::ios::out | std::ios::trunc);

//     for (unsigned k = 0; k < csi.size(); ++k) {
//         float re = csi[k].real();
//         float im = csi[k].imag();
//         fout.write(reinterpret_cast<const char*>(&re), sizeof(float));
//         fout.write(reinterpret_cast<const char*>(&im), sizeof(float));
//     }
//     fout.close();
// }

// /**************************************************************************************************/




/// \brief Looks at the output of the validator and, if unsuccessful, fills \c msg with the error message.
///
/// This is used to call the validator inside the process methods only if asserts are active.
[[maybe_unused]] static bool handle_validation(std::string& msg, const error_type<std::string>& err)
{
  bool is_success = err.has_value();
  if (!is_success) {
    msg = err.error();
  }
  return is_success;
}

void srs_estimator_generic_impl::compensate_phase_shift(span<cf_t> mean_lse,
                                                        float      phase_shift_subcarrier,
                                                        float      phase_shift_offset)
{
  unsigned sequence_length = mean_lse.size();

  // Generate phase indices.
  span<unsigned> phase_indices = span<unsigned>(temp_phase).first(sequence_length);
  std::generate(
      phase_indices.begin(), phase_indices.end(), [phase_shift_subcarrier, phase_shift_offset, n = 0]() mutable {
        return static_cast<int>(std::round(static_cast<float>(cexp_table_size) *
                                           (static_cast<float>(n++) * phase_shift_subcarrier + phase_shift_offset) /
                                           TWOPI));
      });

  // Generate complex exponential.
  span<cf_t> cexp = span<cf_t>(temp_cexp).first(sequence_length);
  cexp_table.generate(cexp, phase_indices);

  // Compensate phase shift.
  srsvec::prod(mean_lse, cexp, mean_lse);
}

srs_estimator_result srs_estimator_generic_impl::estimate(const resource_grid_reader&        grid,
                                                          const srs_estimator_configuration& config)
{
  // Makes sure the PDU is valid.
  [[maybe_unused]] std::string msg;
  srsran_assert(handle_validation(msg, srs_validator_generic_impl(max_nof_prb).is_valid(config)), "{}", msg);

  unsigned nof_rx_ports         = config.ports.size();
  auto     nof_antenna_ports    = static_cast<unsigned>(config.resource.nof_antenna_ports);
  auto     nof_symbols          = static_cast<unsigned>(config.resource.nof_symbols);
  unsigned nof_symbols_per_slot = get_nsymb_per_slot(cyclic_prefix::NORMAL);
  srsran_assert(config.resource.start_symbol.value() + nof_symbols <= nof_symbols_per_slot,
                "The start symbol index (i.e., {}) plus the number of symbols (i.e., {}) exceeds the number of symbols "
                "per slot (i.e., {})",
                config.resource.start_symbol,
                nof_symbols,
                nof_symbols_per_slot);

  // Extract subcarrier spacing.
  subcarrier_spacing scs = to_subcarrier_spacing(config.slot.numerology());

  // Extract comb size.
  auto comb_size = static_cast<unsigned>(config.resource.comb_size);

  srs_information common_info = get_srs_information(config.resource, 0);

  // Sequence length is common for all ports and symbols.
  unsigned sequence_length = common_info.sequence_length;

  // Maximum measurable delay due to cyclic shift.
  double max_ta = 1.0 / static_cast<double>(common_info.n_cs_max * scs_to_khz(scs) * 1000 * comb_size);

  // Prepare results.
  srs_estimator_result result;
  result.time_alignment.time_alignment = 0;
  result.time_alignment.resolution     = 0;
  result.time_alignment.min            = std::numeric_limits<double>::min();
  result.time_alignment.max            = std::numeric_limits<double>::max();
  result.channel_matrix                = srs_channel_matrix(nof_rx_ports, nof_antenna_ports);

  // Temporary LSE.
  static_tensor<3, cf_t, max_seq_length * srs_constants::max_nof_rx_ports * srs_constants::max_nof_tx_ports> temp_lse(
      {sequence_length, nof_rx_ports, nof_antenna_ports});

  // All sequences of pilots.
  static_tensor<2, cf_t, max_seq_length * srs_constants::max_nof_tx_ports> all_sequences(
      {sequence_length, nof_antenna_ports});

  // Auxiliary buffer for noise computation.
  static_tensor<3, cf_t, 2 * max_seq_length * srs_constants::max_nof_rx_ports> temp_noise(
      {sequence_length, 2, nof_rx_ports});
  srsvec::zero(temp_noise.get_data());

  srs_information info_port0         = get_srs_information(config.resource, /*antenna_port*/ 0);
  bool            interleaved_pilots = (nof_antenna_ports == 4) && (info_port0.n_cs >= info_port0.n_cs_max / 2);

  float epre = 0;
  // Iterate transmit ports.
  for (unsigned i_antenna_port = 0; i_antenna_port != nof_antenna_ports; ++i_antenna_port) {
    // Obtain SRS information for a given SRS antenna port.
    srs_information info = get_srs_information(config.resource, i_antenna_port);

    // Generate sequence and store them in all_sequences.
    span<cf_t> sequence = all_sequences.get_view({i_antenna_port});
    deps.sequence_generator->generate(sequence, info.sequence_group, info.sequence_number, info.n_cs, info.n_cs_max);

    // For the current Tx antenna, keep track of all the LSEs at all Rx ports.
    modular_re_buffer_reader<cf_t, srs_constants::max_nof_rx_ports> port_lse(nof_rx_ports, sequence_length);

    // Iterate receive ports.
    for (unsigned i_rx_port_index = 0; i_rx_port_index != nof_rx_ports; ++i_rx_port_index) {
      unsigned i_rx_port = config.ports[i_rx_port_index];

      // View to the mean LSE for a port combination.
      span<cf_t> mean_lse = temp_lse.get_view({i_rx_port_index, i_antenna_port});
      // View for noise computation: with interleaved pilots, we need to keep track of two different sets of REs - those
      // for odd-indexed ports and those for even-indexed ports.
      span<cf_t> noise_help = temp_noise.get_view({(interleaved_pilots) ? i_antenna_port % 2 : 0U, i_rx_port_index});

      // Extract sequence for all symbols and average LSE.
      for (unsigned i_symbol     = config.resource.start_symbol.value(),
                    i_symbol_end = config.resource.start_symbol.value() + nof_symbols;
           i_symbol != i_symbol_end;
           ++i_symbol) {
        // Extract received sequence.
        static_vector<cf_t, max_seq_length> rx_sequence(info.sequence_length);
        grid.get(rx_sequence, i_rx_port, i_symbol, info.mapping_initial_subcarrier, info.comb_size);

        // Since the same SRS sequence is sent over all symbols, it makes sense to average out the noise. When pilots
        // are interleaved, we need to keep track of two different sets of REs.
        if ((i_antenna_port == 0) || (interleaved_pilots && (i_antenna_port == 1))) {
          srsvec::add(noise_help, rx_sequence, noise_help);
          epre += srsvec::average_power(rx_sequence);
        }

        // Avoid accumulation for the first symbol containing SRS.
        if (i_symbol == config.resource.start_symbol.value()) {
          srsvec::copy(mean_lse, rx_sequence);
        } else {
          srsvec::add(mean_lse, rx_sequence, mean_lse);
        }
      }

      // Calculate LSE.
      srsvec::prod_conj(mean_lse, mean_lse, sequence);

      // Scale accumulated LSE.
      if (nof_symbols > 1) {
        srsvec::sc_prod(mean_lse, mean_lse, 1.0 / static_cast<float>(nof_symbols));
      }

      port_lse.set_slice(i_rx_port_index, mean_lse);
    }

    // Estimate TA. Note that, since port_lse still contains the contributions of the other Tx ports (which cancel out
    // only when averaging across subcarriers), the channel impulse response of the channel will show a number of
    // replicas. However, since the TA estimator picks the peak closest to the origin (i.e., the one corresponding to
    // the first replica), the estimation is still valid.
    time_alignment_measurement ta_meas = deps.ta_estimator->estimate(port_lse, info.comb_size, scs, max_ta);

    // Combine time alignment measurements.
    result.time_alignment.time_alignment += ta_meas.time_alignment;
    result.time_alignment.min        = std::max(result.time_alignment.min, ta_meas.min);
    result.time_alignment.max        = std::min(result.time_alignment.max, ta_meas.max);
    result.time_alignment.resolution = std::max(result.time_alignment.resolution, ta_meas.resolution);
  }

  // Average time alignment across all paths.
  result.time_alignment.time_alignment /= nof_antenna_ports;

    // =========================================================================
    // Call the new logging function here
    dump_time_alignment(result.time_alignment.time_alignment);
    // =========================================================================


  float noise_var = 0;
  float rsrp      = 0;
  // Compensate time alignment and estimate channel coefficients.
  for (unsigned i_rx_port = 0; i_rx_port != nof_rx_ports; ++i_rx_port) {
    for (unsigned i_antenna_port = 0; i_antenna_port != nof_antenna_ports; ++i_antenna_port) {
      // View to the mean LSE for a port combination.
      span<cf_t> mean_lse = temp_lse.get_view({i_rx_port, i_antenna_port});

      // Get sequence information.
      srs_information info = get_srs_information(config.resource, i_antenna_port);

      // Calculate subcarrier phase shift in radians.
      auto phase_shift_subcarrier =
          static_cast<float>(TWOPI * result.time_alignment.time_alignment * scs_to_khz(scs) * 1000 * comb_size);

      // Calculate the initial phase shift in radians.
      float phase_shift_offset =
          phase_shift_subcarrier * static_cast<float>(info.mapping_initial_subcarrier) / static_cast<float>(comb_size);

      // Compensate phase shift.
      compensate_phase_shift(mean_lse, phase_shift_subcarrier, phase_shift_offset);

/*****************************************************************************************/
      
      // //std::cout << "Hello, CSI debug!" << std::endl;
      
      // //dump_csi_txt(mean_lse, i_rx_port, i_antenna_port);
      // dump_csi_bin(mean_lse, i_rx_port, i_antenna_port);
      
      // dump_csi_bin(mean_lse, i_rx_port, i_antenna_port);
      
      // static bool initialized = false;
      
      // if (!initialized) {
      //   init_sctp_sender("35.12.214.63", 5000);
      //   initialized = true;
      // }
      
      
      // send_csi_sctp(mean_lse, i_rx_port, i_antenna_port);


/*****************************************************************************************/


/*****************************************************************************************/
      // Data dump and asynchronous SCTP send
      //std::cout << "Hello, CSI debug!" << std::endl;

      // Dump CSI to a binary file (this remains a synchronous operation)
      dump_csi_bin(mean_lse, i_rx_port, i_antenna_port);
      
      // Queue CSI data for sending over SCTP by the background thread.
      // This call is non-blocking.
      sctp_transmitter.queue_csi_data(mean_lse, i_rx_port, i_antenna_port, result.time_alignment.time_alignment);

/*****************************************************************************************/



      // Calculate channel wideband coefficient.
      cf_t coefficient = srsvec::mean(mean_lse);
      result.channel_matrix.set_coefficient(coefficient, i_rx_port, i_antenna_port);
      rsrp += std::norm(coefficient);

      // View for noise computation: with interleaved pilots, we need to keep track of two different sets of REs -
      // those for odd-indexed ports and those for even-indexed ports.
      span<cf_t> noise_help = temp_noise.get_view({(interleaved_pilots) ? i_antenna_port % 2 : 0U, i_rx_port});

      if ((i_antenna_port == 0) || (interleaved_pilots && (i_antenna_port == 1))) {
        compensate_phase_shift(noise_help, phase_shift_subcarrier, phase_shift_offset);
      }

      // We recover the signal by multiplying the SRS sequence by the channel coefficient and we remove it from
      // noise_help. Recall that the latter contains the contribution of all symbols, so the reconstructed symbol must
      // also be counted nof_symbols times.
      static_vector<cf_t, max_seq_length> recovered_signal(noise_help.size());
      srsvec::sc_prod(
          recovered_signal, all_sequences.get_view({i_antenna_port}), static_cast<float>(nof_symbols) * coefficient);
      srsvec::subtract(noise_help, noise_help, recovered_signal);
    }
    span<cf_t> noise_help = temp_noise.get_view({0U, i_rx_port});
    noise_var += srsvec::average_power(noise_help) * noise_help.size();

    if (interleaved_pilots) {
      noise_help = temp_noise.get_view({1U, i_rx_port});
      noise_var += srsvec::average_power(noise_help) * noise_help.size();
    }
  }
  // At this point, noise_var contains the sum of all the squared errors between the received signal and the
  // reconstructed one. For each Rx port, the number of degrees of freedom used to estimate the channel coefficients
  // is usually equal nof_antenna_ports, but when pilots are interleaved, in which case it's 2. Also, when
  // interleaving pilots, we look at double the samples.
  unsigned nof_estimates     = (interleaved_pilots ? 2 : nof_antenna_ports);
  unsigned correction_factor = (interleaved_pilots ? 2 : 1);
  noise_var /= static_cast<float>((nof_symbols * sequence_length - nof_estimates) * correction_factor * nof_rx_ports);

  // Normalize the wideband channel matrix with respect to the noise standard deviation, so that the Frobenius norm
  // square will give us a rough estimate of the SNR. Avoid huge coefficients if the noise variance is too low
  // (keep SNR <= 40 dB).
  float noise_std = std::max(std::sqrt(noise_var), std::sqrt(rsrp) * 0.01F);
  result.channel_matrix *= 1 / noise_std;

  epre /= static_cast<float>(nof_symbols * correction_factor * nof_rx_ports);
  rsrp /= static_cast<float>(nof_antenna_ports * nof_rx_ports);

  // Set noise variance, EPRE and RSRP.
  result.noise_variance = noise_var;
  result.epre_dB        = convert_power_to_dB(epre);
  result.rsrp_dB        = convert_power_to_dB(rsrp);

  return result;
}
