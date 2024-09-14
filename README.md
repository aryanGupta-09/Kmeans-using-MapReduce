# Kmeans-using-MapReduce
A scalable K-Means clustering system with MapReduce paradigm and gRPC communication. This project showcases the seamless coordination between masters, mappers, and reducers to enable efficient data analysis, fostering cluster discovery and insights.

## Tech Stack
<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Languages/Python.svg" width="45" height="45" alt="Python" /></a>
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/Protobuf.png" width="64" height="38" alt="Protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/gRPC.png" width="75" height="38" alt="gRPC" /></a>

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

5. Run the Python files
