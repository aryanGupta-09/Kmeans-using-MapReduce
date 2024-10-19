# Kmeans-using-MapReduce

Implemented the K-means clustering algorithm using the MapReduce framework, coordinating mappers for parallel processing and reducers for result aggregation. Integrated iterative detection to halt processing upon convergence and ensured fault tolerance by reassigning tasks during failures, achieving a 99% task completion rate.

## Tech Stack
<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Languages/Python.svg" width="45" height="45" alt="Python" /></a>
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/Protobuf.png" width="64" height="38" alt="Protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/gRPC.png" width="75" height="38" alt="gRPC" /></a>

## Implementation Details

1. **Master**
    - Responsible for running and communicating with other components in the system.
    - Requires the following parameters as input:
        - Number of mappers (M)
        - Number of reducers (R)
        - Number of centroids (K)
        - Number of iterations for K-Means (program stops if the algorithm converges before)
        - Other necessary information

2. **Input Split (invoked by master)**
    - The master divides the input data into smaller chunks for parallel processing by mappers.
    - Each mapper processes a different chunk, with the master attempting to split the data equally based on the number of mappers.

3. **Map (invoked by mapper)**
    - Applies the Map function to each input split to generate intermediate key-value pairs.
    - Each mapper reads its assigned input split independently.
    - **Inputs to the Map function:**
        - Input split assigned by the master
        - List of centroids from the previous iteration
        - Other necessary information
    - **Output from the Map function:**
        - Key: Index of the nearest centroid to the data point
        - Value: The data point itself
    - The output is passed to the partition function and written to a partition file in the mapper’s directory.
    - Mappers run in parallel as separate processes.

4. **Partition (invoked by mapper)**
    - Partitions the output of the Map function into smaller partitions.
    - Ensures:
        - All key-value pairs for the same key go to the same partition file.
        - Different keys are distributed equally among partitions (e.g., using `key % num_reducers`).
    - Each mapper creates M*R partitions in total.

5. **Shuffle and Sort (invoked by reducer)**
    - Sorts intermediate key-value pairs by key and groups values belonging to the same key.
    - Sends key-value pairs to reducers based on the key.

6. **Reduce (invoked by reducer)**
    - Receives intermediate key-value pairs, applies shuffle & sort, and produces final key-value pairs.
    - **Input to the reduce function:**
        - Key: Centroid ID
        - Value: List of all data points belonging to this centroid ID
        - Other necessary information
    - **Output of the reduce function:**
        - Key: Centroid ID
        - Value: Updated Centroid
    - Each reducer runs as a separate process and writes output to its directory.

7. **Centroid Compilation (invoked by master)**
    - The master compiles the final list of K centroids from reducer outputs and stores it in a file.
    - Before the first iteration, centroids should be randomly selected from input data points.
    - The master uses gRPC calls to read output files from reducers, ensuring efficient data handling.

8. **Fault Tolerance and RPCs**
    - If a mapper or reducer fails, the master re-runs that task to ensure computations finish.
    - gRPC communication between components includes:
        - Master ⇔ Mapper (master sends parameters to mapper)
        - Master ⇔ Reducer (master invokes reducers after all mappers return)
        - Reducer ⇔ Mapper (reducers communicate with mappers for input before shuffle, sort, and reduce)
        - Master ⇔ Reducer (master contacts reducers to read output data files after completion)

9. **Print Statements & Log Generation**
    - During the execution of the master program for each iteration, key data is printed and logged, including:
        - Iteration number
        - Execution of gRPC calls to Mappers or Reducers
        - gRPC responses for each Mapper and Reducer function (SUCCESS/FAILURE)
        - Centroids generated after each iteration (including the randomly initialized centroids)

## Installation

1. Clone the repo
```bash
  git clone https://github.com/aryanGupta-09/MapReduce.git
```

2. Go to the project directory
```bash
  cd MapReduce
```

3. Generate the Python code for gRPC
```bash
  python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kmeans.proto
```

4. Update Input/points.txt

5. Initialize the number of mappers and run mapper.py

6. Initialize the number of reducers and run reducer.py

7. Initialise the number of centroids, mappers and reducers, and run master.py
