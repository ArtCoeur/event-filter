var logger = require('./lib/logger'),
    rabbitmq = require('rabbit.js'),
    Logstash = require('logstash-client');

logger.info('event-filter: running');

// wait until rabbitmq can accept connections, somehow
function doConnect(){

    try {
        var context = rabbitmq.createContext(
            'amqp://' + process.env.RABBITMQ_PORT_5672_TCP_ADDR + ':' + process.env.RABBITMQ_PORT_5672_TCP_PORT
        );

        var logstash = new Logstash({
            type: 'tcp', // udp, tcp, memory
            host: process.env.LOGSTASH_PORT_,
            port: process.env.LOGSTASH_PORT_
        });

        context.on('ready', function() {
            logger.info('event-filter: connected');

            // subscribe to pub and sub queues
            var sub = context.socket('SUB'),
                pub = context.socket('PUB');

            sub.connect('events', function(){
                // deal with facts as they come in
                sub.on('data', function (body) {
                    // send to logstash
                    logstash.send(body);
                });
            });
        });

        return true;

    } catch (err){
        logger.error(err);

        setTimeout(doConnect, 3000);
        return false;
    }
}

// hack to wait till rabbitmq is up
setTimeout(doConnect, 10000);
