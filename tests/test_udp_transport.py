import socket
import time
import pytest
from dv_routing.udp_transport import (
    UDPTransport,
    MSG_TYPE_ADVERT,
    MSG_TYPE_DATA,
    MSG_TYPE_CONTROL,
)


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_constants():
    assert MSG_TYPE_ADVERT == "ADVERT"
    assert MSG_TYPE_DATA == "DATA"
    assert MSG_TYPE_CONTROL == "CONTROL"


def test_send_and_receive_json():
    received_msgs_1 = []
    received_msgs_2 = []

    port1 = get_free_port()
    port2 = get_free_port()
    while port2 == port1:
        port2 = get_free_port()

    t1 = UDPTransport("127.0.0.1", port1, lambda payload, addr: received_msgs_1.append((payload, addr)))
    t2 = UDPTransport("127.0.0.1", port2, lambda payload, addr: received_msgs_2.append((payload, addr)))

    try:
        t1.start()
        t2.start()

        msg_from_1 = {
            "type": MSG_TYPE_ADVERT,
            "router_id": "R1",
            "routes": {"R1": 0, "R2": 2},
        }
        t1.send_json("127.0.0.1", port2, msg_from_1)

        # Wait for delivery
        timeout = time.time() + 2.0
        while not received_msgs_2 and time.time() < timeout:
            time.sleep(0.01)

        assert len(received_msgs_2) == 1
        payload2, addr2 = received_msgs_2[0]
        assert payload2 == msg_from_1
        assert addr2[0] == "127.0.0.1"
        assert addr2[1] == port1

        # Send response from 2 to 1
        msg_from_2 = {
            "type": MSG_TYPE_DATA,
            "source": "R2",
            "destination": "R1",
            "payload": "Ping test",
        }
        t2.send_json("127.0.0.1", port1, msg_from_2)

        timeout = time.time() + 2.0
        while not received_msgs_1 and time.time() < timeout:
            time.sleep(0.01)

        assert len(received_msgs_1) == 1
        payload1, addr1 = received_msgs_1[0]
        assert payload1 == msg_from_2
        assert addr1[0] == "127.0.0.1"
        assert addr1[1] == port2

    finally:
        t1.close()
        t2.close()


def test_clean_shutdown():
    port = get_free_port()
    t = UDPTransport("127.0.0.1", port, lambda p, a: None)
    t.start()
    assert t._thread is not None
    assert t._thread.is_alive()

    t.close()
    assert not t._thread.is_alive()
    # Idempotent close
    t.close()


def test_malformed_packets_do_not_crash_listener():
    received_msgs = []
    port = get_free_port()
    t = UDPTransport("127.0.0.1", port, lambda payload, addr: received_msgs.append((payload, addr)))

    try:
        t.start()

        # Send raw non-JSON and invalid UTF-8 bytes using a raw socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as raw_sock:
            raw_sock.sendto(b"\xff\xfe\xfd\x00corrupt", ("127.0.0.1", port))
            raw_sock.sendto(b"{invalid_json: 123", ("127.0.0.1", port))

        time.sleep(0.1)

        # Thread must still be alive
        assert t._thread.is_alive()

        # Now send a valid message and ensure it is processed
        valid_msg = {"type": MSG_TYPE_CONTROL, "command": "STATUS"}
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as raw_sock:
            import json
            raw_sock.sendto(json.dumps(valid_msg).encode("utf-8"), ("127.0.0.1", port))

        timeout = time.time() + 2.0
        while not received_msgs and time.time() < timeout:
            time.sleep(0.01)

        assert len(received_msgs) == 1
        assert received_msgs[0][0] == valid_msg

    finally:
        t.close()
