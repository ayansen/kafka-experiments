# kafka-experiments
This is a repository for trying out different features around kafka and kafka-streams. 


## Build
The code uses [docker-compose](./docker-compose.yml) to spin up  kafka and its dependencies and then runs the integration tests on them to experiment the features.
To achieve that using maven it uses the docker-compose plugin for maven and then running the integration tests using the failsafe plugin
To try out the tests run
 - `mvn clean verify`

## Debug
To debug the tests, just run the command
 -  `docker-compose up` on your terminal
 -  Run the test in debug mode from your IDE

## Experiments
 - Ordering of events across partitions - [Test](./kafka-consumer-experiments/src/test/kotlin/ayansen/programming/kafka/experiments/EventOrderingIT.kt) to validate kakfa events are ordered in a partition 
   
