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
        $this->histogramTable->column('value', Table::TYPE_INT, 8);
        $this->histogramTable->create();
    }

    /**
     * @return MetricFamilySamples[]
     */
    public function collect(): array
    {
        $metrics = $this->internalCollect($this->counterTable);
        $metrics = array_merge($metrics, $this->internalCollect($this->gaugeTable));
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
        foreach ($this->histogramTable as $key => $row) {
            [$name, $help, $type, $labelNames, $labelValues, $buckets, $bucketType] = explode(":", $key);
            $key = implode(':', [$name, $type, $labelValues, $bucketType]);
            if (isset($data[$key])) {
                $data[$key] = [
                    'name' => $name,
                    'help' => $help,
                    'type' => $type,
                    'labelNames' => $this->decode($labelNames),
                    'buckets' => $this->decode($buckets),
                ];
                // Add the Inf bucket so we can compute it later on
                $data[$key]['buckets'][] = '+Inf';
            }
            $data[$key]['histogramBuckets'][$labelValues][$bucketType] = $row['value'];
        }
        foreach ($data as $item) {
            // Compute all buckets
            $labels = array_keys($item['histogramBuckets']);
            sort($labels);
            foreach ($labels as $labelValues) {
                $acc = 0;
                $decodedLabelValues = $this->decode($labelValues);
                foreach ($item['buckets'] as $bucket) {
                    $bucket = (string)$bucket;
                    if (!isset($item['histogramBuckets'][$labelValues][$bucket])) {
                        $item['samples'][] = [
                            'name' => $item['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    } else {
                        $acc += $item['histogramBuckets'][$labelValues][$bucket];
                        $item['samples'][] = [
                            'name' => $item['name'] . '_' . 'bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    }
                }

                // Add the count
                $item['samples'][] = [
                    'name' => $item['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $acc,
                ];

                // Add the sum
                $item['samples'][] = [
                    'name' => $item['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $item['histogramBuckets'][$labelValues]['sum'],
                ];

            }
            $histograms[] = new MetricFamilySamples($item);
        }
        return $histograms;
    }

    /**
     * @param Table $metrics
     * @return array
     */
    private function internalCollect(Table $metrics): array
    {
        $result = [];
        $data = [];
        foreach ($metrics as $key => $row) {
            [$name, $help, $type, $labelNames, $labelValues] = explode(":", $key);
            $key = implode(":", [$name, $type]);
            if (!isset($result[$key])) {
                $data[$key] = [
                    'name' => $name,
                    'help' => $help,
                    'type' => $type,
                    'labelNames' => $this->decode($labelNames),
                ];
            }
            $data[$key]['samples'][] = [
                'name' => $name,
                'labelNames' => [],
                'labelValues' => $this->decode($labelValues),
                'value' => $row['value'],
            ];
        }
        foreach ($data as $item) {
            $this->sortSamples($item['samples']);
            $result[] = new MetricFamilySamples($item);
        }
        return $result;
    }

    /**
     * @param array $data
     * @return void
     */
    public function updateHistogram(array $data): void
    {
        $sumKey = $this->histogramBucketValueKey($data, 'sum');
        $this->histogramTable->incr($sumKey, 'value', $data['value']);

        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }

        $bucketKey = $this->histogramBucketValueKey($data, $bucketToIncrease);
        $this->histogramTable->incr($sumKey, 'value', 1);
    }

    /**
     * @param array $data
     */
    public function updateGauge(array $data): void
    {
        $internalKey = $this->internalKey($data);
        if (!$this->gaugeTable->exist($internalKey)) {
            $this->gaugeTable->set($internalKey, ['value' => 0]);
        }
        if ($data['command'] === Adapter::COMMAND_SET) {
            $this->gaugeTable->set($internalKey, ['value' => $data['value']]);
        } else {
            $this->gaugeTable->incr($internalKey, 'value', $data['value']);
        }
    }

    /**
     * @param array $data
     */
    public function updateCounter(array $data): void
    {
        $internalKey = $this->internalKey($data);
        if (!$this->counterTable->exist($internalKey) || $data['command'] === Adapter::COMMAND_SET) {
            $this->counterTable->set($internalKey, ['value' => 0]);
        } else {
            $this->counterTable->incr($internalKey, 'value', $data['value']);
        }
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
            $data['help'],
            $data['type'],
            $this->encode($data['labelNames']),
            $this->encode($data['labelValues']),
            $this->encode($data['buckets']),
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
            $data['help'],
            $data['type'],
            $this->encode($data['labelNames']),
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
        $json = \msgpack_pack($values);
        return base64_encode($json);
    }

    /**
     * @param $values
     * @return array
     */
    private function decode($values): array
    {
        $json = base64_decode($values, true);
        $decodedValues = \msgpack_unpack($json);
        return $decodedValues;
    }

}