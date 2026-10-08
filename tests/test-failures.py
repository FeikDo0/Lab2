
import uuid
import pytest
from replication import ReplicationManager


def test_three_replicas():
    manager = ReplicationManager([50051, 50052, 50053])

    result = manager.replicate(
        counter_id="test_three",
        delta=5,
        key=str(uuid.uuid4())
    )

    assert result["success"] is True
    assert result["acknowledgements"] == 3


def test_one_replica_failure():
    manager = ReplicationManager([50051, 50052, 59999])

    result = manager.replicate(
        counter_id="test_one_failure",
        delta=5,
        key=str(uuid.uuid4())
    )

    assert result["success"] is True
    assert result["acknowledgements"] == 2


def test_two_replica_failures():
    manager = ReplicationManager([50051, 59998, 59999])

    result = manager.replicate(
        counter_id="test_two_failures",
        delta=5,
        key=str(uuid.uuid4())
    )

    assert result["success"] is False
    assert result["acknowledgements"] == 1
