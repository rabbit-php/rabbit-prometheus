<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\CollectorRegistry;
use rabbit\App;

/**
 * Class ServerCollecter
 * @package Rabbit\Prometheus
 */
class ServerCollecter
{
    /** @var CollectorRegistry */
    protected $registry;

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
     */
    public function collectWorker(int $workerId): void
    {
        $gauge = $this->registry->getOrRegisterGauge(CollectHelper::WORKER, "status", CollectHelper::HELP, [CollectHelper::LABLE]);
        $gauge->set(App::getServer()->getSwooleServer()->getWorkerStatus(), [$workerId]);
        CollectHelper::collectMem($this->registry, $workerId);
        CollectHelper::collectTimer($this->registry, $workerId);
        CollectHelper::collectCoroutine($this->registry, $workerId);
        CollectHelper::collectPool($this->registry, $workerId);
    }

    public function collectServer(): void
    {
        $list = App::getServer()->getSwooleServer()->stats();
        foreach ($list as $key => $value) {
            $counter = $this->registry->getOrRegisterGauge('server', 'stats', CollectHelper::HELP, ['type']);
            $counter->set($value, [$key]);
        }
    }
}