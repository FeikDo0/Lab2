
import grpc
import counter_pb2
import counter_pb2_grpc


class ReplicationManager:

    def __init__(self, ports):
        self.ports = ports
        self.quorum = len(ports) // 2 + 1

    def replicate(self, counter_id, delta, key):
        acknowledgements = 0
        results = []

        for port in self.ports:
            try:
                with grpc.insecure_channel(
                    f"localhost:{port}"
                ) as channel:

                    stub = counter_pb2_grpc.CounterStub(channel)

                    request = counter_pb2.IncrementRequest(
                        counter_id=counter_id,
                        delta=delta,
                        idempotency_key=key
                    )

                    response = stub.Increment(
                        request,
                        timeout=2.0
                    )

                    acknowledgements += 1

                    results.append({
                        "port": port,
                        "value": response.new_value,
                        "duplicate": response.was_duplicate
                    })

            except grpc.RpcError:
                print(f"Replica {port} unavailable")

        return {
            "success": acknowledgements >= self.quorum,
            "acknowledgements": acknowledgements,
            "results": results
        }
