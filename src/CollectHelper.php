<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\CollectorRegistry;
use Prometheus\Exception\MetricsRegistrationException;
use Rabbit\Pool\PoolManager;

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
     * @throws MetricsRegistrationException
     */
    public static function collectMem(CollectorRegistry $registry, int $workerId): void
    {
        $gauge = $registry->getOrRegisterGauge(self::WORKER, "mem_usage", self::HELP, [self::LABLE]);
        $peak = $registry->getOrRegisterGauge(self::WORKER, "mem_peak_usage", self::HELP, [self::LABLE]);
        $gauge->set(memory_get_usage(false), [$workerId]);
        $peak->set(memory_get_usage(true), [$workerId]);
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws MetricsRegistrationException
     */
    public static function collectTimer(CollectorRegistry $registry, int $workerId): void
    {
        $gauge = $registry->getOrRegisterGauge(self::WORKER, "timer_count", self::HELP, [self::LABLE]);
        $gauge->set(count(\Rabbit\Base\Core\Timer::getTimers()), [$workerId]);
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws MetricsRegistrationException
     */
    public static function collectCoroutine(CollectorRegistry $registry, int $workerId): void
    {
        foreach (\Co::stats() as $key => $value) {
            $gauge = $registry->getOrRegisterGauge(self::WORKER, "coroutine_$key", self::HELP, [self::LABLE]);
            $gauge->set($value, [$workerId]);
        }
    }

    /**
     * @param CollectorRegistry $registry
     * @param int $workerId
     * @throws MetricsRegistrationException
     */
    public static function collectPool(CollectorRegistry $registry, int $workerId): void
    {
        foreach (PoolManager::getPools() as $pool) {
            if (method_exists($pool, 'getServiceList')) {
                $addrList = [];
                foreach ($pool->getServiceList() as $uri) {
                    $parsed_url = parse_url($uri);
                    $scheme = isset($parsed_url['scheme']) ? $parsed_url['scheme'] . '://' : '';
                    $host = isset($parsed_url['host']) ? $parsed_url['host'] : '';
                    $port = isset($parsed_url['port']) ? ':' . $parsed_url['port'] : '';
                    $addrList[] = $pool->getPoolConfig()->getName() . "-$scheme$host$port";
                }
                $mgauge = $registry->getOrRegisterGauge(self::WORKER, "pool_num", self::HELP, [self::LABLE, 'pool_dsn']);
                $current = $registry->getOrRegisterGauge(self::WORKER, "pool_current", self::HELP, [self::LABLE, 'pool_dsn']);
                $gauge = $registry->getOrRegisterGauge(self::WORKER, "pool_idle", self::HELP, [self::LABLE, 'pool_dsn']);
                $labels = [$workerId, implode(',', $addrList)];
                $gauge->set($pool->getPool()->length(), $labels);
                $mgauge->set($pool->getPoolConfig()->getMinActive(), $labels);
                $current->set($pool->getCurrentCount(), $labels);
            }
        }
    }
}