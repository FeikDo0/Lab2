
from recovery import ReplicaRecovery


recovery = ReplicaRecovery([50051, 50052, 50053])

value = recovery.get_latest_value("replicated_counter")

print("Recovered value:", value)

assert value is not None

print("RECOVERY LOOKUP PASSED")
