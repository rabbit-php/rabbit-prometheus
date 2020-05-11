<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Exception;
use Prometheus\CollectorRegistry;
use Prometheus\RenderTextFormat;
use rabbit\App;
use rabbit\httpclient\Client;

/**
 * Class PushGateway
 * @package Rabbit\Prometheus
 */
class PushGateway extends \Prometheus\PushGateway
{
    /**
     * @var string
     */
    private $address;
    /** @var Client */
    private $client;

    protected $renderer;

    /**
     * PushGateway constructor.
     * @param $address
     */
    public function __construct($address)
    {
        $this->address = $address;
        $this->client = new Client([
            'use_pool' => true
        ]);
        $this->renderer = new RenderTextFormat();
    }

    /**
     * @param CollectorRegistry $collectorRegistry
     * @param string $job
     * @param array|null $groupingKey
     * @throws Exception
     */
    public function push(CollectorRegistry $collectorRegistry, string $job, array $groupingKey = null): void
    {
        $this->doRequest($collectorRegistry, $job, $groupingKey, 'put');
    }

    /**
     * @param CollectorRegistry $collectorRegistry
     * @param string $job
     * @param array|null $groupingKey
     * @throws Exception
     */
    public function pushAdd(CollectorRegistry $collectorRegistry, string $job, array $groupingKey = null): void
    {
        $this->doRequest($collectorRegistry, $job, $groupingKey, 'post');
    }

    /**
     * @param string $job
     * @param array|null $groupingKey
     * @throws Exception
     */
    public function delete(string $job, array $groupingKey = null): void
    {
        $this->doRequest(null, $job, $groupingKey, 'delete');
    }

    /**
     * @param CollectorRegistry $collectorRegistry
     * @param string $job
     * @param array $groupingKey
     * @param string $method
     * @throws Exception
     */
    private function doRequest(CollectorRegistry $collectorRegistry, string $job, array $groupingKey, $method): void
    {
        $url = "http://" . $this->address . "/metrics/job/" . $job;
        if (!empty($groupingKey)) {
            foreach ($groupingKey as $label => $value) {
                $url .= "/" . $label . "/" . $value;
            }
        }
        $requestOptions = [
            'headers' => [
                'Content-Type' => RenderTextFormat::MIME_TYPE,
            ],
            'timeout' => 20,
        ];
        if ($method != 'delete') {
            $requestOptions['body'] = $this->renderer->render($collectorRegistry->getMetricFamilySamples());
        }
        $response = $this->client->request(array_merge(['method' => $method, 'uri' => $url], $requestOptions));
        $statusCode = $response->getStatusCode();
        if (!in_array($statusCode, [200, 202])) {
            $msg = "Unexpected status code "
                . $statusCode
                . " received from push gateway "
                . $this->address . ": " . $response->getBody();
            App::error($msg);
        }
    }
}