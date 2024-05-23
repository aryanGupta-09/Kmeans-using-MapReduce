import grpc
import kmeans_pb2
import kmeans_pb2_grpc
import random
import threading
import queue
import os
import shutil
import math

class Master:
    
    def __init__(self, num_mappers, num_reducers, num_centroids, max_iterations, mappers, reducers):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_centroids = num_centroids
        self.max_iterations = max_iterations
        self.mappers = mappers
        self.reducers = reducers

    # def start_MapRed(self):
    #     cmd = f'start cmd /k python mapper2.py {self.num_mappers}'
    #     subprocess.Popen(cmd, shell=True)
    #     cmd = f'start cmd /k python reducer2.py {self.num_reducers}'
    #     subprocess.Popen(cmd, shell=True)
    
    def dump_state(self, message):
        directory = 'Dump'
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with open(f'{directory}/TotalDump.txt', 'a') as f:
                f.write(f'{message}\n')
        except Exception as e:
            print(f"An error occurred while dumping the state")

    def delete_old_files(self):
        # Delete the old partition files
        for i in range(self.num_mappers):
            directory = f'Mappers/mapper_{i}'
            for j in range(self.num_reducers):
                filename = f'{directory}/partition_{j}.txt'
                if os.path.exists(filename):
                    os.remove(filename)

        # Delete the old output files
        for i in range(self.num_reducers):
            directory = f'Reducers/reducer_{i}'
            filename = f'{directory}/output.txt'
            if os.path.exists(filename):
                os.remove(filename)

    def split_input_data(self):
        with open('Input/points.txt', 'r') as f:
            lines = f.readlines()
        total_lines = len(lines)
        lines_per_chunk = total_lines // self.num_mappers
        indices = [(i * lines_per_chunk, (i + 1) * lines_per_chunk if i != self.num_mappers - 1 else total_lines) for i in range(self.num_mappers)]
        return indices

    def send_to_mappers(self, indices, centroids):
        print("\nMapper:")
        self.dump_state("\n|Sending data to mappers|")

        threads = []
        failed_tasks = queue.Queue()

        for i, (start, end) in enumerate(indices):
            t = threading.Thread(target=self.send_to_mapper, args=(i, start, end, centroids, failed_tasks))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        
        # Retry failed tasks with different mappers
        while not failed_tasks.empty():
            i, start, end, centroids = failed_tasks.get()
            i = (i + 1) % self.num_mappers # Choose a different mapper
            self.send_to_mapper(i, start, end, centroids, failed_tasks)

    def send_to_mapper(self, i, start, end, centroids, failed_tasks):
        for attempt in range(2):
            try:
                self.dump_state(f"Sending data to mapper {i} - Lines {start} to {end}")
                with grpc.insecure_channel(self.mappers[i]) as channel:
                    stub = kmeans_pb2_grpc.KMeansStub(channel)
                    request = kmeans_pb2.MapRequest(mapper_id=i, num_reducers=self.num_reducers, start=start, end=end, centroids=centroids)
                    response = stub.Map(request)
                    if response.success:
                        print(f"Data successfully sent to mapper {i}")
                        self.dump_state(f"Data successfully sent to mapper {i}")
                        return True
                    else:
                        print(f"Attempt {attempt + 1} failed for mapper {i} : {response.error}")
                        self.dump_state(f"Attempt {attempt + 1} failed for mapper {i} : {response.error}")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for mapper {i} : {e}")
        
        else:
            print(f"Failed to send data to mapper {i} after 2 attempts")
            self.dump_state(f"Failed to send data to mapper {i} after 2 attempts")
            failed_tasks.put((i, start, end, centroids))
        return False

    def get_from_reducers(self):
        print("\nReducer:")
        self.dump_state("\n|Getting data from reducers|")
        centroids = []
        threads = []
        lock = threading.Lock()
        failed_tasks = queue.Queue()

        for i in range(self.num_reducers):
            t = threading.Thread(target=self.get_from_reducer, args=(i, i, centroids, lock, failed_tasks))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        
        # # Retry failed tasks with different reducers
        while not failed_tasks.empty():
            failed_id = failed_tasks.get()
            reducer_id = (i + 1) % self.num_reducers
            self.get_from_reducer(reducer_id, failed_id, centroids, lock, failed_tasks)

        # Sort the centroids list based on the first value of parts
        centroids.sort(key=lambda x: x[0]) 
        # Create a new list that only contains the Centroid objects
        centroids = [x[1] for x in centroids]
        return centroids
    
    def get_from_reducer(self, reducer_id, failed_id, centroids, lock, failed_tasks):
        for attempt in range(3):
            try:
                with grpc.insecure_channel(self.reducers[reducer_id]) as channel:
                    stub = kmeans_pb2_grpc.KMeansStub(channel)
                    self.dump_state(f"Grpc call to reducer {reducer_id}")#getting the data from reducer
                    request = kmeans_pb2.ReduceRequest(reducer_id=reducer_id, failed_id=failed_id, mappers=self.mappers)
                    response = stub.Reduce(request)

                    if response.success:
                        self.dump_state(f"Data successfully received from reducer {reducer_id}")
                        # Convert each line of the output to a Centroid object
                        for line in response.output.split('\n'):
                            parts = line.split(',')
                            if len(parts) >= 3:
                                coordinates = list(map(float, parts[1:3]))
                                centroid = kmeans_pb2.Centroid(coordinates=coordinates)
                                # Append a tuple containing the first value of parts and the centroid
                                with lock:
                                    centroids.append((parts[0], centroid))
                        return
                    else:
                        print(f"Attempt {attempt + 1} failed: {response.error}")
                        self.dump_state(f"Attempt {attempt + 1} failed: {response.error}")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
        
        else:
            print(f"Failed to get data from reducer {failed_id} after 3 attempts")
            self.dump_state(f"Failed to get data from reducer {failed_id} after 3 attempts")
            failed_tasks.put(failed_id)

    def write_centroids_to_file(self, output):
        # Write the lines to the file
        with open('centroids.txt', 'w') as f:
            for centroid in output:
                values = ','.join(map(str, centroid.coordinates))
                f.write(values + '\n')

    def has_converged(self, old_centroids, new_centroids, threshold):
        for i in range(len(old_centroids)):
            diff_x = abs(old_centroids[i].coordinates[0] - new_centroids[i].coordinates[0])
            diff_y = abs(old_centroids[i].coordinates[1] - new_centroids[i].coordinates[1])
            diff = math.sqrt(diff_x**2 + diff_y**2)
            if diff > threshold:
                return False
        return True

    def run(self):
        # self.start_MapRed()
        # sleep(10)
        old_centroids = []
        self.dump_state("Starting the KMeans algorithm - Iteration 0")
        directory = f'Dump/Iteration{0}'
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Select centroids randomly from data.txt
        with open('Input/points.txt', 'r') as f:
            data = [list(map(float, line.strip().split(','))) for line in f]
        data = random.sample(data, self.num_centroids)
        for centroid in data:
            c = kmeans_pb2.Centroid(coordinates = [float(x) for x in centroid])
            old_centroids.append(c)

        indices = self.split_input_data()

        self.send_to_mappers(indices, old_centroids)        
        os.makedirs(f'Dump/Iteration0/Mappers/', exist_ok=True)
        for i in range(self.num_mappers):
            os.makedirs(f'Dump/Iteration0/Mappers/mapper_{i}/', exist_ok=True)
            for j in range(self.num_reducers):
                shutil.copy(f'Mappers/mapper_{i}/partition_{j}.txt', f'Dump/Iteration0/Mappers/mapper_{i}/partition_{j}.txt')

        new_centroids = self.get_from_reducers()
        os.makedirs('Dump/Iteration0/Reducers', exist_ok=True)
        for i in range(self.num_reducers):
            shutil.copy(f'Reducers/reducer_{i}/output.txt', f'Dump/Iteration0/Reducers/reducer_{i}.txt')

        self.write_centroids_to_file(new_centroids)        
        with open('Dump/Iteration0/centroid.txt', 'w') as f:
            for centroid in new_centroids:
                values = ','.join(map(str, centroid.coordinates))
                f.write(values + '\n')
        
        iteration = 1
        while not self.has_converged(old_centroids, new_centroids, threshold=0.01):
            self.dump_state(f"\n||Iteration {iteration}||")
            os.makedirs(f'Dump/Iteration{iteration}', exist_ok=True)
            print(f"\n Iteration {iteration}")
            old_centroids = new_centroids
            self.delete_old_files()
            indices = self.split_input_data()

            self.send_to_mappers(indices, old_centroids)
            for i in range(self.num_mappers):
                os.makedirs(f'Dump/Iteration{iteration}/Mappers/mapper_{i}/', exist_ok=True)
                for j in range(self.num_reducers):
                    shutil.copy(f'Mappers/mapper_{i}/partition_{j}.txt', f'Dump/Iteration{iteration}/Mappers/mapper_{i}/partition_{j}.txt')

            new_centroids = self.get_from_reducers()
            os.makedirs(f'Dump/Iteration{iteration}/Reducers', exist_ok=True)
            for i in range(self.num_reducers):
                shutil.copy(f'Reducers/reducer_{i}/output.txt', f'Dump/Iteration{iteration}/Reducers/reducer_{i}.txt')

            self.write_centroids_to_file(new_centroids)
            with open(f'Dump/Iteration{iteration}/centroid.txt', 'w') as f:
                for centroid in new_centroids:
                    values = ','.join(map(str, centroid.coordinates))
                    f.write(values + '\n')

            iteration += 1
            if iteration == self.max_iterations:
                print(f"Maximum number of iterations reached ({self.max_iterations})")
                break
        print(f"Converged after {iteration} iterations")

if __name__ == '__main__':
    num_mappers = 5
    num_reducers = 5
    num_centroids = 2
    max_iterations = 20
    mappers = [f'localhost:5005{i+1}' for i in range(num_mappers)]
    reducers = [f'localhost:5006{i+1}' for i in range(num_reducers)]
    master = Master(num_mappers, num_reducers, num_centroids, max_iterations, mappers, reducers)
    master.run()