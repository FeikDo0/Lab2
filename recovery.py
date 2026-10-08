
import grpc

import counter_pb2
import counter_pb2_grpc


class ReplicaRecovery:

    def __init__(self, ports):
        self.ports = ports

    def get_latest_value(self, counter_id):
        values = []

        for port in self.ports:
            try:
                with grpc.insecure_channel(
                    f"localhost:{port}"
                ) as channel:

                    stub = counter_pb2_grpc.CounterStub(channel)

                    response = stub.Get(
                        counter_pb2.GetRequest(
                            counter_id=counter_id
                        ),
                        timeout=2.0
                    )

                    if response.found:
                        values.append(response.value)

            except grpc.RpcError:
                print(f"Replica {port} unavailable")

        if not values:
            return None

        return max(values)

    def restore_replicas(self, counter_id, source_port):
        try:
            with grpc.insecure_channel(
                f"localhost:{source_port}"
            ) as channel:

                stub = counter_pb2_grpc.CounterStub(channel)

                response = stub.Get(
                    counter_pb2.GetRequest(
                        counter_id=counter_id
                    ),
                    timeout=2.0
                )

                if not response.found:
                    print("Source counter not found")
                    return False

                value = response.value

        except grpc.RpcError:
            print("Source replica unavailable")
            return False

        print(f"Source value: {value}")

        success = True

        for port in self.ports:
            if port == source_port:
                continue

            try:
                with grpc.insecure_channel(
                    f"localhost:{port}"
                ) as channel:

                    stub = counter_pb2_grpc.CounterStub(channel)

                    reply = stub.Restore(
                        counter_pb2.RestoreRequest(
                            counter_id=counter_id,
                            value=value,
                            version=0,
                            lamport_time=0
                        ),
                        timeout=2.0
                    )

                    if reply.success:
                        print(f"Replica {port} restored: {value}")
                    else:
                        success = False

            except grpc.RpcError:
                print(f"Replica {port} unavailable")
                success = False

        return success
