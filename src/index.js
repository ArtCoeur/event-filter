var logger = require('./lib/logger'),
    rabbitmq = require('rabbit.js'),
    elasticsearch = require('elasticsearch');


logger.info('running');

var context = rabbitmq.createContext(
    'amqp://' + process.env.RABBITMQ_PORT_5672_TCP_ADDR + ':' + process.env.RABBITMQ_PORT_5672_TCP_PORT
);

context.on('ready', function() {
    logger.info('connected');

    // subscribe to 'events' queue
    var sub = context.socket('SUB');

    sub.connect('events', function(){
        // deal with facts as they come in
        sub.on('data', function (body) {

            logger.info(body);

            var fact = JSON.parse(body);

            var client = new elasticsearch.Client({
                host: 'elasticsearch:9200',
                log: 'trace'
            });

            // send fact object to es not a string
            client.index({
                type: fact.name,
                index: "facts",
                body: fact
            }, function(err, response){
                logger.error('client response err: ' + err + " response: " + response);
            });
        });
    });
});

