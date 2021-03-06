<?php

declare(strict_types=1);

namespace Rabbit\Prometheus;

use Rabbit\Base\Core\Exception;
use Rabbit\Base\Core\Timer;
use Rabbit\Server\ServerHelper;
use Rabbit\Server\WorkerHandlerInterface;

/**
 * Class PushWorker
 * @package Rabbit\Prometheus
 */
class ServerPusher implements WorkerHandlerInterface
{
    /** @var int */
    protected int $tick;
    /** @var ServerCollecter */
    protected ServerCollecter $collector;
    /** @var PushGateway */
    protected PushGateway $pushGateway;

    /**
     * PushWorker constructor.
     * @param ServerCollecter $collector
     * @param PushGateway $pushGateway
     * @param int $tick
     */
    public function __construct(ServerCollecter $collector, PushGateway $pushGateway, int $tick = 60)
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
        $server = ServerHelper::getServer()->getSwooleServer();
        Timer::addTickTimer($this->tick * 1000, function () use ($worker_id) {
            $this->collector->collectWorker($worker_id);
        }, 'prom.collect');

        if ((isset($server->setting['task_worker_num']) && $worker_id === $server->setting['worker_num']) ||
            (!isset($server->setting['task_worker_num']) && $worker_id === 0)
        ) {
            Timer::addTickTimer($this->tick * 1000, function () {
                $this->collector->collectServer();
                $this->pushGateway->push($this->collector->getRegistry(), getDI('appName', false, "Rabbit"), ['instance' => current(swoole_get_local_ip())]);
            }, 'prom.push');
        }
    }
}
