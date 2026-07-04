FROM rabbitmq:3.13-management
RUN rabbitmq-plugins enable rabbitmq_stomp

# RabbitMQ STOMP extension
# To use, run this instead of the commands in ./rabbit.sh
#
# docker run -d --name rabbitmq-stomp -p 61613:61613 -p 5672:5672 -p 15672:15672 rabbitmq-stomp