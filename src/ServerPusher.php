<?php

declare(strict_types=1);

namespace Rabbit\Prometheus;

use Exception;
use rabbit\App;
use rabbit\server\WorkerHandlerInterface;
use Swoole\Timer;

/**
 * Class PushWorker
 * @package Rabbit\Prometheus
 */
class ServerPusher implements WorkerHandlerInterface
{
    /** @var int */
    protected $tick;
    /** @var ServerCollecter */
    protected $collector;
    /** @var PushGateway */
    protected $pushGateway;

    /**
     * PushWorker constructor.
     * @param ServerCollecter $collector
     * @param PushGateway $pushGateway
     * @param int $tick
     */
    public function __construct(ServerCollecter $collector, PushGateway $pushGateway, int $tick = 5)
    {
        $this->collector = $collector;
        $this->pushGateway = $pushGateway;
        $this->tick = $tick;
    }

    /**
     * @param int $worker_id
     * @throws Exception
     */
    public function handle(int $worker_id): void
    {
        $server = App::getServer()->getSwooleServer();
        Timer::tick($this->tick * 1000, function () use ($worker_id) {
            $this->collector->collectWorker($worker_id);
        });

        if ((isset($server->setting['task_worker_num']) && $worker_id === $server->setting['worker_num']) ||
            (!isset($server->setting['task_worker_num']) && $worker_id === 0)) {
            Timer::tick($this->tick * 1500, function () {
                $this->collector->collectServer();
                $this->pushGateway->push($this->collector->getRegistry(), getDI('appName', false, "Rabbit"), ['instance' => current(swoole_get_local_ip())]);
            });
        }
    }

}