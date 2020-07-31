<?php
declare(strict_types=1);

namespace Rabbit\Prometheus;

use Prometheus\CollectorRegistry;
use Prometheus\RenderTextFormat;
use Rabbit\Base\App;
use Rabbit\HttpClient\Client;
use Swlib\Saber\Request;
use Throwable;

/**
 * Class PushGateway
 * @package Rabbit\Prometheus
 */
class PushGateway
{
    private string $address;
    private Client $client;
    protected RenderTextFormat $renderer;
    protected bool $onLog = false;

    /**
     * PushGateway constructor.
     * @param $address
     */
    public function __construct(string $address)
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
     * @throws Throwable
     */
    public function push(CollectorRegistry $collectorRegistry, string $job, array $groupingKey = null): void
    {
        $this->doRequest($collectorRegistry, $job, $groupingKey, 'put');
    }

    /**
     * @param CollectorRegistry $collectorRegistry
     * @param string $job
     * @param array|null $groupingKey
     * @throws Throwable
     */
    public function pushAdd(CollectorRegistry $collectorRegistry, string $job, array $groupingKey = null): void
    {
        $this->doRequest($collectorRegistry, $job, $groupingKey, 'post');
    }

    /**
     * @param string $job
     * @param array|null $groupingKey
     * @throws Throwable
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
     * @throws Throwable
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
        $this->onLog && $requestOptions['before'] = [
            function (Request $request) {
                $uri = $request->getUri();
                App::info(
                    sprintf(
                        "Request %s %s",
                        $request->getMethod(),
                        $uri->getScheme() . "://" . $uri->getHost() . (($port = $uri->getPort()) ? ":$port" : '') . $uri->getPath()
                    ),
                    "http"
                );
            },
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