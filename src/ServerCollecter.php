<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\CollectorRegistry;
use Prometheus\Exception\MetricsRegistrationException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Server\ServerHelper;

/**
 * Class ServerCollecter
 * @package Rabbit\Prometheus
 */
class ServerCollecter
{
    /** @var CollectorRegistry */
    protected CollectorRegistry $registry;

    /**
     * ServerCollecter constructor.
     * @param CollectorRegistry $registry
     */
    public function __construct(CollectorRegistry $registry)
    {
        $this->registry = $registry;
    }

    /**
     * @return CollectorRegistry
     */
    public function getRegistry(): CollectorRegistry
    {
        return $this->registry;
    }

    /**
     * @param int|null $workerId
     * @throws MetricsRegistrationException
     */
    public function collectWorker(int $workerId): void
    {
        $gauge = $this->registry->getOrRegisterGauge(CollectHelper::WORKER, "status", CollectHelper::HELP, [CollectHelper::LABLE]);
        $gauge->set(ServerHelper::getServer()->getSwooleServer()->getWorkerStatus(), [$workerId]);
        CollectHelper::collectMem($this->registry, $workerId);
        CollectHelper::collectTimer($this->registry, $workerId);
        CollectHelper::collectCoroutine($this->registry, $workerId);
        CollectHelper::collectPool($this->registry, $workerId);
    }

    public function collectServer(): void
    {
        $server = ServerHelper::getServer()->getSwooleServer();
        $list = $server->stats();
        $counter = $this->registry->getOrRegisterGauge('server', 'stats', CollectHelper::HELP, ['type']);
        foreach ($list as $key => $value) {
            $counter->set($value, [$key]);
        }
        $counter->set(ArrayHelper::getValue($server->setting, 'task_worker_num', 0), ['task_worker_num']);
    }
}