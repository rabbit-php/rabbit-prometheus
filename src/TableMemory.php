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

    /** @var null|Table */
    private $counterMap = null;
    /** @var null|Table */
    private $gaugeMap = null;
    /** @var null|Table */
    private $histogramMap = null;

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

        $this->counterMap = new Table($counterLine);
        $this->counterMap->column('value', Table::TYPE_STRING, 65535);
        $this->counterMap->create();

        $this->gaugeTable = new Table($gagueLine);
        $this->gaugeTable->column('value', Table::TYPE_INT, 8);
        $this->gaugeTable->create();

        $this->gaugeMap = new Table($counterLine);
        $this->gaugeMap->column('value', Table::TYPE_STRING, 65535);
        $this->gaugeMap->create();

        $this->histogramTable = new Table($histogramLine);
        $this->histogramTable->column('value', Table::TYPE_FLOAT);
        $this->histogramTable->create();

        $this->histogramMap = new Table($counterLine);
        $this->histogramMap->column('value', Table::TYPE_STRING, 65535);
        $this->histogramMap->create();
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
        foreach ($this->histogramMap as $metaData) {
            $metaData = json_decode($metaData['value'], true);
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
                [$name, $labelValues, $bucket] = explode(":", $key);
                if ($name === $metaData['name']) {
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
     * @param Table $metrics
     * @param Table $table
     * @return array
     */
    private function internalCollect(Table $metrics, Table $table): array
    {
        $result = [];
        foreach ($metrics as $metric) {
            $metaData = json_decode($metric['value'], true);
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
            ];
            foreach ($table as $key => $value) {
                [$name, $labelValues] = explode(':', $key);
                if ($name === $metaData['name']) {
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
        if (!$this->histogramMap->exist($data['name'])) {
            $this->histogramMap->set($data['name'], ['value' => json_encode($this->metaData($data))]);
        }
        // Initialize the sum
        $sumKey = $this->histogramBucketValueKey($data, 'sum');
        $this->histogramTable->incr($sumKey, 'value', (float)$data['value']);

        // Figure out in which bucket the observation belongs
        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }

        $bkey = $this->histogramBucketValueKey($data, $bucketToIncrease);
        $this->histogramTable->incr($bkey, 'value', (float)1);
    }

    /**
     * @param array $data
     */
    public function updateGauge(array $data): void
    {
        $valueKey = $this->internalKey($data);
        if (!$this->gaugeMap->exist($data['name'])) {
            $this->gaugeMap->set($data['name'], ['value' => json_encode($this->metaData($data))]);
        }
        if ($data['command'] == Adapter::COMMAND_SET) {
            $this->gaugeTable->set($valueKey, ['value' => (int)$data['value']]);
        } else {
            $this->gaugeTable->incr($valueKey, 'value', (int)$data['value']);
        }
    }

    /**
     * @param array $data
     */
    public function updateCounter(array $data): void
    {
        if (!$this->counterMap->exist($data['name'])) {
            $this->counterMap->set($data['name'], ['value' => json_encode($this->metaData($data))]);
        }
        $this->counterTable->incr($this->internalKey($data), 'value', (int)$data['value']);
    }

    /**
     * @param array $data
     * @return string
     */
    private function metaKey(array $data): string
    {
        return $data['name'];
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