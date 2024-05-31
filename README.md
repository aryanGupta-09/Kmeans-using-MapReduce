# Kmeans-using-MapReduce
Building a scalable K-Means clustering system with MapReduce paradigm and gRPC communication. This project showcases the seamless coordination between masters, mappers, and reducers to enable efficient data analysis, fostering cluster discovery and insights.

## Tech Stack
<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://raw.githubusercontent.com/danielcranney/readme-generator/main/public/icons/skills/python-colored.svg" width="45" height="45" alt="Python" /></a>
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer"><img src="https://www.techunits.com/wp-content/uploads/2021/07/pb.png" height="40" alt="protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/aryanGupta-09/assets/96881807/310cb125-1346-49b9-a87a-b6a84934a9a6" width="78" height="40" alt="gRPC" /></a>

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
