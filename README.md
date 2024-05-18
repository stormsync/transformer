# Transformer

## Overview
Transformer is a microservice designed to take hail, tornado, and wind reports that are already on a kafka 
topic (the collector microservice does this work)and handle data transformation between raw text, apply needed logic, 
and  place back on another kafka topic in a standardized format.

The performance can be improved as needed by utilizing channels during processing which would allow for some
concurrency when reading in raw lines and getting them transformed and put back on a topic.   The utilization is pretty
low and that hasn't been any issues regarding  speed, so I did  not feel the need to optimize prematurely.  

## Features
- **Data Consumption**: Reads data from a kafka topic.
- **Transformation**: Applies transformation rules to data.
- **Data Provision**: Outputs transformed data to specified kafka topic.
- **Protobuf Integration**: Utilizes protocol buffers for efficient serialization.

## Requirements
- Go 1.16+
- Docker (optional, for containerized execution)

## Installation
Clone the repository:
```sh
git clone https://github.com/stormsync/transformer.git
cd transformer
```
Install dependencies:
```sh
go mod tidy
```

## Usage
### Building the Project
```sh
go build -o transformer cmd/transform/main.go
```

### Running the Transformer
```sh
KAFKA_ADDRESS="address:port" \
KAFKA_USER="username" \ 
KAFKA_PASSWORD="password" \
PROVIDER_TOPIC="topic-name"  \
CONSUMER_TOPIC="another-topic-name" \
./transformer
```

### Docker
Build the Docker image:
```sh
docker build -t transformer:<tag> .
```
Run the Docker container:
```sh
docker run  -e "KAFKA_ADDRESS=address:port" -e "KAFKA_USER=username" -e "KAFKA_PASSWORD=password" -e "CONSUMER_TOPIC=topic" -e"PROVIDER_TOPIC=another-topic" transformer:<tag> 
```

## Directory Structure
- `cmd/transform/`: Main application entry point.
- `proto/`: Protocol buffer definitions.
- `report/`: Storm report types and conversion functions.
- `transform.go`: Core transformation logic.
- `consumer.go`: Kafka consumer logic.
- `provider.go`: Kafka provider logic.

## Contributing
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.

---

For more information, visit the [GitHub repository](https://github.com/stormsync/transformer).