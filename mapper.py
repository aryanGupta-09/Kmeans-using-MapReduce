import grpc
import kmeans_pb2
import kmeans_pb2_grpc
import numpy as np
import os
from concurrent import futures
import multiprocessing
import random

class Mapper(kmeans_pb2_grpc.KMeansServicer):
    
    def partition(self, key_value_pairs, num_reducers, mapper_id):
        # Partition the key-value pairs into R partitions, where R is the number of reducers
        partitions = [[] for _ in range(num_reducers)]
        for key, value in key_value_pairs:
            partitions[key % num_reducers].append((key, value))
        
        lines_written = []
        for i, partition in enumerate(partitions):
            directory = f'Mappers/mapper_{mapper_id}'
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(f'{directory}/partition_{i}.txt', 'a') as f:
                for key, value in partition:
                    f.write(f'{key},{",".join(map(str, value))}\n')
                    lines_written.append((i, f'{key},{",".join(map(str, value))}\n'))
        return lines_written
    
    def distance(self, point1, point2):
        return np.sqrt(np.sum((np.array(point1) - np.array(point2)) ** 2))

    def map(self, input_split, centroids):
        result = []
        for line in input_split:
            data_point = list(map(float, line.strip().split(',')))
            distances = [self.distance(data_point, centroid) for centroid in centroids]
            nearest_centroid_index = np.argmin(distances)
            result.append((nearest_centroid_index, data_point))
        return result

    def Map(self, request, context):
        input_split = []
        # Read the specified portion of the input file
        with open(f'Input/points.txt', 'r') as f:
            for i, line in enumerate(f):
                if i < request.start:
                    continue
                if i >= request.end:
                    break
                input_split.append(line)

        # Apply the map function to the input split
        key_value_pairs = list(self.map(input_split, [centroid.coordinates for centroid in request.centroids]))
        lines_written  = self.partition(key_value_pairs, request.num_reducers, request.mapper_id)
        # Add a probabilistic flag
        success = random.choice([True, False])
        print(f"Reading lines {request.start} to {request.end} for mapper {request.mapper_id}. {success}")

        if success:
            return kmeans_pb2.MapResponse(success=True, error='')
        # Delete the lines written during this task
        for i, line in lines_written:
            with open(f'Mappers/mapper_{request.mapper_id}/partition_{i}.txt', 'r') as f:
                lines = f.readlines()
            with open(f'Mappers/mapper_{request.mapper_id}/partition_{i}.txt', 'w') as f:
                for l in lines:
                    if l != line:
                        f.write(l)
        return kmeans_pb2.MapResponse(success=False, error='Probabilistic failure')
    
    def ReducerInput(self, request, context):
        # The directory where the partition files are stored
        directory = f'Mappers/mapper_{request.mapper_id}'
        # The partition file for this reducer
        filename = f'{directory}/partition_{request.reducer_id}.txt'
        # Read the partition file and return the key-value pairs
        key_values = []
        with open(filename, 'r') as f:
            for line in f:
                key, *values = map(float, line.strip().split(','))
                key_values.append(kmeans_pb2.KeyValues(key=key, values=values))

        return kmeans_pb2.ReducerInputResponse(key_values=key_values)

def serve(mapper_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kmeans_pb2_grpc.add_KMeansServicer_to_server(Mapper(), server)
    server.add_insecure_port(f'localhost:5005{mapper_id+1}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # num_mappers = sys.argv[1] # Change this to the number of mappers you want
    num_mappers = 5 # Change this to the number of mappers you want
    processes = []
    for i in range(num_mappers):
        process = multiprocessing.Process(target=serve, args=(i,))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()