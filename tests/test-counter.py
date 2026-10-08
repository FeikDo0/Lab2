
import threading
import grpc
import uuid
import pytest
from concurrent import futures

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import call_with_retry


@pytest.fixture
def running_server():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(), server
    )

    port = server.add_insecure_port("localhost:0")
    server.start()

    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = counter_pb2_grpc.CounterStub(channel)
        yield stub

    server.stop(grace=0).wait()


def test_increment_applies_delta(running_server):
    stub = running_server

    request = counter_pb2.IncrementRequest(
        counter_id="x",
        delta=5,
        idempotency_key=str(uuid.uuid4())
    )

    response = stub.Increment(request, timeout=2.0)

    assert response.new_value == 5
    assert response.was_duplicate is False


def test_duplicate_key_not_reapplied(running_server):
    stub = running_server

    request = counter_pb2.IncrementRequest(
        counter_id="x",
        delta=5,
        idempotency_key="test-key-123"
    )

    response1 = stub.Increment(request, timeout=2.0)
    response2 = stub.Increment(request, timeout=2.0)

    assert response1.new_value == 5
    assert response1.was_duplicate is False

    assert response2.new_value == 5
    assert response2.was_duplicate is True

    result = stub.Get(
        counter_pb2.GetRequest(counter_id="x"),
        timeout=2.0
    )

    assert result.value == 5


def test_concurrent_increments_exact(running_server):
    stub = running_server
    errors = []

    def increment_1000():
        try:
            for i in range(1000):
                request = counter_pb2.IncrementRequest(
                    counter_id="concurrent",
                    delta=1,
                    idempotency_key=str(uuid.uuid4())
                )

                stub.Increment(request, timeout=2.0)
        except Exception as error:
            errors.append(error)

    thread1 = threading.Thread(target=increment_1000)
    thread2 = threading.Thread(target=increment_1000)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    assert not errors

    response = stub.Get(
        counter_pb2.GetRequest(counter_id="concurrent"),
        timeout=2.0
    )

    assert response.found is True
    assert response.value == 2000


def test_get_missing_counter(running_server):
    stub = running_server

    request = counter_pb2.GetRequest(
        counter_id="unknown_counter"
    )

    response = stub.Get(request, timeout=2.0)

    assert response.found is False
    assert response.value == 0


def test_retry_after_timeout_is_safe(running_server):
    stub = running_server

    request = counter_pb2.IncrementRequest(
        counter_id="timeout_test",
        delta=10,
        idempotency_key="timeout-key-123"
    )

    with pytest.raises(grpc.RpcError) as error:
        stub.Increment(request, timeout=0.1)

    assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED

    response = call_with_retry(
        stub,
        stub.Increment,
        request
    )

    assert response.new_value == 10
    assert response.was_duplicate is True

    result = stub.Get(
        counter_pb2.GetRequest(counter_id="timeout_test"),
        timeout=2.0
    )

    assert result.found is True
    assert result.value == 10
