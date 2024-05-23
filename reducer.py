import grpc
import kmeans_pb2
import kmeans_pb2_grpc
from concurrent import futures
from collections import defaultdict
import os
import multiprocessing
import random

class Reducer(kmeans_pb2_grpc.KMeansServicer):
    
    def sort_and_shuffle(self, key_value_pairs):
        # Shuffle and sort the key-value pairs by key
        key_value_pairs.sort(key=lambda pair: pair[0])

        # Group the values that belong to the same key
        grouped_values = defaultdict(list)
        for key, values in key_value_pairs:
            grouped_values[key].append(values)
        
        return grouped_values

    def reduce(self, grouped_values):
        # Perform the reduce operation
        # Calculate the updated centroid for each key
        reduced_values = {key: [sum(group)/len(group) for group in zip(*values)] for key, values in grouped_values.items()}
        return reduced_values

    def Reduce(self, request, context):
        key_value_pairs = []
        # Create a stub for the Mapper service
        for i in range(len(request.mappers)):
            try:
                with grpc.insecure_channel(request.mappers[i]) as channel:
                    stub = kmeans_pb2_grpc.KMeansStub(channel)

                    # Request the output of each mapper
                    response = stub.ReducerInput(kmeans_pb2.ReducerInputRequest(mapper_id = i, reducer_id = request.failed_id))
                    
                    # Convert each KeyValues object to a tuple and append it to the list
                    for kv in response.key_values:
                        key_value_pairs.append((kv.key, list(kv.values)))
            
            except Exception as e:
                print(f"Failed to connect to the Mapper service: {e}")
                return kmeans_pb2.ReduceResponse(success=False, error=str(e), output='')
        
        # Sort and shuffle the key-value pairs
        grouped_values = self.sort_and_shuffle(key_value_pairs)

        # Reduce the key-value pairs
        reduced_values = self.reduce(grouped_values)

        # Convert the reduced values to a string
        output = '\n'.join(f'{key},{",".join(map(str, values))}' for key, values in reduced_values.items())+ '\n'

        directory = f'Reducers/reducer_{request.reducer_id}'
        if not os.path.exists(directory):
            os.makedirs(directory)
        # Add a probabilistic flag
        success = random.choice([True, False])
        if success:
            with open(f'{directory}/output.txt', 'a') as f:
                f.write(output)
            return kmeans_pb2.ReduceResponse(success=True, error='', output=output)
        
        with open(f'{directory}/output.txt', 'a') as f:
            f.write('')
        return kmeans_pb2.ReduceResponse(success=False, error='Probabilistic Error', output='')

def serve(reducer_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kmeans_pb2_grpc.add_KMeansServicer_to_server(Reducer(), server)
    server.add_insecure_port(f'localhost:5006{reducer_id+1}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # num_reducers = sys.argv[1] # Change this to the number of mappers you want
    num_reducers = 5  # Change this to the number of mappers you want
    processes = []
    for i in range(num_reducers):
        process = multiprocessing.Process(target=serve, args=(i,))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()