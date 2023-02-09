<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;


/*
    size - return the current number of jobs on the queue,
    push - push a new job on to the queue
    pushRaw - push a raw payload on to the queue
    later - push a new job on the queue to be processed later
    pushOn - push a new job on to a specific queue
    laterOn - push a new job on a given queue to be processed later
    pop - take a job from the queue
*/



class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer;
    protected $producer;
    
    public function __construct($producer,$consumer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method
    }

    public function push($job, $data = "", $queue = null)
    {
        // TODO: Implement push() method

        $topic = $this->producer->newTopic($queue ?? env("KAFKA_QUEUE"));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);//basically send
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method
    }

    public function later($delay, $job, $data = "", $queue = null)
    {
        // TODO: Implement later() method
    }

    public function pop($queue = null)
    {
        // TODO: Implement pop() method
        var_dump('queue loop start');
        
        $this->consumer->subscribe([$queue]);

        try{
            $message = $this->consumer->consume(12*1000);

            switch($message->err){
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump($message->payload);
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump( "NO MORE MESSAGES, WAIT FOR MORE\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump( "Timed out\n");
                    break;
                default:
                    throw new \Exception($message->errstr(), $message-err);
                    break;
            }

        } catch (\Exception $e){
            var_dump($e->getMessage());
        }
      

        var_dump('queue loop end');
    }


}