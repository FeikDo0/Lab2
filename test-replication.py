
import uuid
from replication import ReplicationManager


manager = ReplicationManager(
    [50051, 50052, 50053]
)

result = manager.replicate(
    counter_id="replicated_counter",
    delta=5,
    key=str(uuid.uuid4())
)

print("Replication result:", result)

assert result["success"] is True
assert result["acknowledgements"] >= 2

print("QUORUM TEST PASSED")
