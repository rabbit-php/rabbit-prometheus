<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\MetricFamilySamples;
use Prometheus\Storage\Adapter;
use Swoole\Table;

/**
 * Class TableMemory
 * @package Rabbit\Prometheus
 */
class TableMemory implements Adapter
{
    /**
     * @var null|Table
     */
    private $counterTable = null;

    /**
     * @var null|Table
     */
    private $gaugeTable = null;

    /**
     * @var null|Table
     */
    private $histogramTable = null;

    /** @var array */
    private $counterMap = [];
    /** @var array */
    private $gaugeMap = [];
    /** @var array */
    private $histogramMap = [];

    /**
     * TableMemory constructor.
     * @param int $counterLine
     * @param int $gagueLine
     * @param int $histogramLine
     */
    public function __construct(int $counterLine = 1024, int $gagueLine = 1024, int $histogramLine = 1024)
    {
        $this->counterTable = new Table($counterLine);
        $this->counterTable->column('value', Table::TYPE_INT, 8);
        $this->counterTable->create();

        $this->gaugeTable = new Table($gagueLine);
        $this->gaugeTable->column('value', Table::TYPE_INT, 8);
        $this->gaugeTable->create();

        $this->histogramTable = new Table($histogramLine);
        $this->histogramTable->column('value', Table::TYPE_FLOAT);
        $this->histogramTable->create();
    }

    /**
     * @return MetricFamilySamples[]
     */
    public function collect(): array
    {
        $metrics = $this->internalCollect($this->counterMap, $this->counterTable);
        $metrics = array_merge($metrics, $this->internalCollect($this->gaugeMap, $this->gaugeTable));
        $metrics = array_merge($metrics, $this->collectHistograms());
        return $metrics;
    }

    public function flushMemory(): void
    {
        $table = [];
        foreach ($this->counterTable as $key => $column) {
            $table[] = $key;
        }
        foreach ($table as $key) {
            $this->counterTable->del($key);
        }

        $table = [];
        foreach ($this->gaugeTable as $key => $column) {
            $table[] = $key;
        }
        foreach ($table as $key) {
            $this->gaugeTable->del($key);
        }

        $table = [];
        foreach ($this->histogramTable as $key => $column) {
            $table[] = $key;
        }
        foreach ($table as $key) {
            $this->histogramTable->del($key);
        }
    }

    /**
     * @return array
     */
    private function collectHistograms(): array
    {
        $histograms = [];
        $data = [];
        foreach ($this->histogramMap as $histogram) {
            $metaData = $histogram['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'buckets' => $metaData['buckets'],
            ];
            // Add the Inf bucket so we can compute it later on
            $data['buckets'][] = '+Inf';

            $histogramBuckets = [];
            foreach ($this->histogramTable as $key => $value) {
                if (in_array($key, $histogram['samples'])) {
                    $parts = explode(':', $key);
                    $labelValues = $parts[2];
                    $bucket = $parts[3];
                    // Key by labelValues
                    $histogramBuckets[$labelValues][$bucket] = $value['value'];
                }
            }

            // Compute all buckets
            $labels = array_keys($histogramBuckets);
            sort($labels);
            foreach ($labels as $labelValues) {
                $acc = 0;
                $decodedLabelValues = $this->decode($labelValues);
                foreach ($data['buckets'] as $bucket) {
                    $bucket = (string)$bucket;
                    if (!isset($histogramBuckets[$labelValues][$bucket])) {
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    } else {
                        $acc += $histogramBuckets[$labelValues][$bucket];
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_' . 'bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    }
                }

                // Add the count
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $acc,
                ];

                // Add the sum
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $histogramBuckets[$labelValues]['sum'],
                ];

            }
            $histograms[] = new MetricFamilySamples($data);
        }
        return $histograms;
    }

    /**
     * @param array $metrics
     * @param Table $table
     * @return array
     */
    private function internalCollect(array $metrics, Table $table): array
    {
        $result = [];
        foreach ($metrics as $metric) {
            $metaData = $metric['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
            ];
            foreach ($table as $key => $value) {
                if (in_array($key, $metric['samples'])) {
                    $parts = explode(':', $key);
                    $labelValues = $parts[2];
                    $data['samples'][] = [
                        'name' => $metaData['name'],
                        'labelNames' => [],
                        'labelValues' => $this->decode($labelValues),
                        'value' => $value['value'],
                    ];
                }
            }
            $this->sortSamples($data['samples']);
            $result[] = new MetricFamilySamples($data);
        }
        return $result;
    }

    /**
     * @param array $data
     * @return void
     */
    public function updateHistogram(array $data): void
    {
        $metaKey = $this->metaKey($data);
        $sumKey = $this->histogramBucketValueKey($data, 'sum');
        if (array_key_exists($metaKey, $this->histogramMap) === false) {
            $this->histogramMap[$metaKey] = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }
        $this->histogramTable->incr($sumKey, 'value', (float)$data['value']);
        if (!in_array($sumKey, $this->histogramMap[$metaKey]['samples'])) {
            $this->histogramMap[$metaKey]['samples'][] = $sumKey;
        }

        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }

        $bucketKey = $this->histogramBucketValueKey($data, $bucketToIncrease);
        $this->histogramTable->incr($bucketKey, 'value', (float)1);
        if (!in_array($bucketKey, $this->histogramMap[$metaKey]['samples'])) {
            $this->histogramMap[$metaKey]['samples'][] = $bucketKey;
        }
    }

    /**
     * @param array $data
     */
    public function updateGauge(array $data): void
    {
        $metaKey = $this->metaKey($data);
        if (array_key_exists($metaKey, $this->gaugeMap) === false) {
            $this->gaugeMap[$metaKey] = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }
        $internalKey = $this->internalKey($data);
        if ($data['command'] === Adapter::COMMAND_SET) {
            $this->gaugeTable->set($internalKey, ['value' => (int)$data['value']]);
        } else {
            $this->gaugeTable->incr($internalKey, 'value', (int)$data['value']);
        }
        if (!in_array($internalKey, $this->gaugeMap[$metaKey]['samples'])) {
            $this->gaugeMap[$metaKey]['samples'][] = $internalKey;
        }
    }

    /**
     * @param array $data
     */
    public function updateCounter(array $data): void
    {
        $metaKey = $this->metaKey($data);
        if (array_key_exists($metaKey, $this->counterMap) === false) {
            $this->counterMap[$metaKey] = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }
        $internalKey = $this->internalKey($data);
        if ($data['command'] === Adapter::COMMAND_SET) {
            $this->counterTable->set($internalKey, ['value' => 0]);
        } else {
            $this->counterTable->incr($internalKey, 'value', (int)$data['value']);
        }
        if (!in_array($internalKey, $this->counterMap[$metaKey]['samples'])) {
            $this->counterMap[$metaKey]['samples'][] = $internalKey;
        }
    }

    /**
     * @param array $data
     * @return string
     */
    private function metaKey(array $data): string
    {
        return implode(':', [
            $data['type'],
            $data['name']
        ]);
    }

    /**
     * @param array $data
     * @return array
     */
    private function metaData(array $data): array
    {
        $metricsMetaData = $data;
        unset($metricsMetaData['value']);
        unset($metricsMetaData['command']);
        unset($metricsMetaData['labelValues']);
        return $metricsMetaData;
    }

    /**
     * @param array $data
     * @param string $bucket
     * @return string
     */
    private function histogramBucketValueKey(array $data, $bucket): string
    {
        return implode(':', [
            $data['type'],
            $data['name'],
            $this->encode($data['labelValues']),
            $bucket,
        ]);
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function internalKey(array $data): string
    {
        return implode(':', [
            $data['type'],
            $data['name'],
            $this->encode($data['labelValues'])
        ]);
    }

    /**
     * @param array $samples
     */
    private function sortSamples(array &$samples): void
    {
        usort($samples, function ($a, $b) {
            return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
        });
    }

    /**
     * @param array $values
     * @return string
     */
    private function encode(array $values): string
    {
        $json = json_encode($values);
        if (false === $json) {
            throw new \RuntimeException(json_last_error_msg());
        }
        return base64_encode($json);
    }

    /**
     * @param $values
     * @return array
     */
    private function decode($values): array
    {
        $json = base64_decode($values, true);
        if (false === $json) {
            throw new \RuntimeException('Cannot base64 decode label values');
        }
        $decodedValues = json_decode($json, true);
        if (false === $decodedValues) {
            throw new \RuntimeException(json_last_error_msg());
        }
        return $decodedValues;
    }

}