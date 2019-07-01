# RabbitmqBasics
App for basic RabbitMQ concepts demonstration.

Requred external packages:
* https://repo1.maven.org/maven2/com/rabbitmq/amqp-client/5.7.1/amqp-client-5.7.1.jar
* https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.26/slf4j-api-1.7.26.jar
* https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.26/slf4j-simple-1.7.26.jar

To compile:
* javac -cp .:amqp-client-5.7.1.jar:slf4j-api-1.7.26.jar:slf4j-simple-1.7.26.jar ExchangeController.java QueueViewer.java RabbitMqOperator.java RabbitMqReceiver.java

To launch:
* java -cp .:amqp-client-5.7.1.jar:slf4j-api-1.7.26.jar:slf4j-simple-1.7.26.jar ExchangeController
* java -cp .:amqp-client-5.7.1.jar:slf4j-api-1.7.26.jar:slf4j-simple-1.7.26.jar QueueViewer
