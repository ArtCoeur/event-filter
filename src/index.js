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

        logger.info('env' +
        ' : proto: ' + process.env.LOGSTASH_PORT_5000_TCP_PROTO +
        ' : addr: ' + process.env.LOGSTASH_PORT_5000_TCP_ADDR +
        ' : port: ' + process.env.LOGSTASH_PORT_5000_TCP_PORT);

        var logstash = new Logstash({
            type: process.env.LOGSTASH_PORT_5000_TCP_PROTO, // udp, tcp, memory
            host: 'logstash',
            port: process.env.LOGSTASH_PORT_5000_TCP_PORT
        });

        context.on('ready', function() {
            logger.info('event-filter: connected');

            // subscribe to 'events' queue
            var sub = context.socket('SUB');

            sub.connect('events', function(){
                // deal with facts as they come in
                sub.on('data', function (body) {
                    var fact = JSON.parse(body);
                    logger.info('event-filter: fact.name ' + fact.name);
                    // send fact object to logstash not a string
                    logstash.send(fact, function(err, result){
                        logger.info('logstash error: ' + err + ', result: ' + result);
                    });
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
