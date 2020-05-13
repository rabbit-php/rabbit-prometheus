<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\CollectorRegistry;
use rabbit\pool\PoolManager;
use Swoole\Coroutine;
use Swoole\Timer;

/**
 * Class CollectHelper
 * @package Rabbit\Prometheus
 */
class CollectHelper
{
    const WORKER = "worker";
    const HELP = "system";
    const LABLE = "worker_id";

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws \Prometheus\Exception\MetricsRegistrationException
     */
    public static function collectMem(CollectorRegistry $registry, int $workerId): void
    {
        $gauge = $registry->getOrRegisterGauge(self::WORKER, "mem_usage", self::HELP, [self::LABLE]);
        $gauge->set(memory_get_peak_usage(true), [$workerId]);
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws \Prometheus\Exception\MetricsRegistrationException
     */
    public static function collectTimer(CollectorRegistry $registry, int $workerId): void
    {
        $gauge = $registry->getOrRegisterGauge(self::WORKER, "timer_count", self::HELP, [self::LABLE]);
        $gauge->set(count(Timer::list()), [$workerId]);
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws \Prometheus\Exception\MetricsRegistrationException
     */
    public static function collectCoroutine(CollectorRegistry $registry, int $workerId): void
    {
        foreach (Coroutine::stats() as $key => $value) {
            $gauge = $registry->getOrRegisterGauge(self::WORKER, "coroutine_$key", self::HELP, [self::LABLE]);
            $gauge->set($value, [$workerId]);
        }
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws \Prometheus\Exception\MetricsRegistrationException
     */
    public static function collectPool(CollectorRegistry $registry, int $workerId): void
    {
        foreach (PoolManager::getPools() as $pool) {
            $gauge = $registry->getOrRegisterGauge(self::WORKER, "pool_idle", self::HELP, [self::LABLE, 'pool_dsn']);
            $mgauge = $registry->getOrRegisterGauge(self::WORKER, "pool_num", self::HELP, [self::LABLE, 'pool_dsn']);
            $addrList = [];
            foreach ($pool->getServiceList(false, false) as $uri) {
                $parsed_url = parse_url($uri);
                $scheme = isset($parsed_url['scheme']) ? $parsed_url['scheme'] . '://' : '';
                $host = isset($parsed_url['host']) ? $parsed_url['host'] : '';
                $port = isset($parsed_url['port']) ? ':' . $parsed_url['port'] : '';
                $addrList[] = "$scheme$host$port";
            }
            $gauge->set($pool->getUseChannel() ? $pool->getChannelPool()->length() : $pool->getQueuePool()->count(), [$workerId, implode(',', $addrList)]);
            $mgauge->set($pool->getPoolConfig()->getMaxActive(), [$workerId, implode(',', $addrList)]);
        }
    }
}