FROM rabbitmq:3.13-rc-management
RUN rabbitmq-plugins enable rabbitmq_stomp
