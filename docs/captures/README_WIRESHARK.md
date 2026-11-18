# Wireshark Capture Guide: Control Overhead Verification

This 60-second teammate guide details how to capture loopback UDP control traffic across virtual router processes to verify protocol overhead (Section 6.1 and 6.4 of the project report).

---

## Prerequisites
- **Wireshark** installed with the **Npcap** packet capture library (with loopback support enabled).
- Virtual environment activated with project dependencies.

---

## Step-by-Step Capture Procedure (60 Seconds)

1. **Launch Wireshark & Select Interface**
   - Open Wireshark.
   - On the capture interface list, select **`Npcap Loopback Adapter`** (or `Adapter for loopback traffic capture`).
   - Double-click to start packet capture.

2. **Apply Display Filter**
   - In the filter bar at the top, enter the following display filter and press Enter:
     ```wireshark
     udp.port >= 10001 and udp.port <= 10006
     ```
   - *Expected:* Wireshark isolates datagram traffic exchanged between the 6 virtual router processes.

3. **Launch the Multi-Process Network**
   - In a terminal in the project root directory (`d:/Misc Coding/CN Project`), execute:
     ```bash
     python -m dv_routing.launcher --topology topologies/default.txt --interval 2.0
     ```
   - *Expected:* Routers A through F start on UDP ports 10001–10006 and immediately begin exchanging periodic distance-vector advertisement datagrams.

4. **Capture and Stop**
   - Let the network run for **15–20 seconds** while observing the live flow of JSON-encoded routing advertisements in Wireshark.
   - Click the red **Stop Capture** button (or press `Ctrl+E`) in Wireshark.
   - Terminate the router network launcher in your terminal (`Ctrl+C`).

5. **Save Capture File**
   - In Wireshark, select **File $\to$ Save As...**
   - Save the capture file to:
     ```
     docs/captures/control_overhead.pcapng
     ```

---

## Verifying Protocol Overhead
- In Wireshark, open **Statistics $\to$ Protocol Hierarchy** or **Statistics $\to$ Conversations $\to$ UDP** to inspect total packet count and byte volume.
- Each distance-vector advertisement datagram is a JSON payload containing the sender's current distance vector, enabling verification of control-message frequency and byte overhead against theoretical predictions.
