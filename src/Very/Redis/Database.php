<?php

namespace Very\Redis;

use Closure;
use Redis;
use RedisCluster;
use InvalidArgumentException;
use Illuminate\Support\Arr;
use Illuminate\Contracts\Redis\Database as DatabaseContract;

class Database implements DatabaseContract
{
    /**
     * The host address of the database.
     *
     * @var array
     */
    protected $clients;

    protected $cluster = false;

    protected $servers = [];
    protected $options = [];
    protected $currrent_server = 'default';

    /**
     * Create a new Redis connection instance.
     *
     * @param  array $servers
     */
    public function __construct(array $servers = [])
    {
        $this->cluster = Arr::pull($servers, 'cluster');
        $this->options = (array)Arr::pull($servers, 'options');
        $this->servers = $servers;

        if ($this->cluster) {
            $this->clients = $this->createAggregateClient($this->servers, $this->options);
        }
    }


    /**
     * Create a new aggregate client supporting sharding.
     *
     * @param  array  $servers
     * @param  array  $options
     * @return array
     */
    protected function createAggregateClient(array $servers, array $options = [])
    {
        $servers = array_map([$this, 'buildClusterSeed'], $servers);
        $timeout = empty($options['timeout']) ? 0 : $options['timeout'];
        $persistent = isset($options['persistent']) && $options['persistent'];
        return ['default' => new RedisCluster(
            null, array_values($servers), $timeout, null, $persistent
        )];
    }

    /**
     * Build a cluster seed string.
     *
     * @param  array  $server
     * @return string
     */
    protected function buildClusterSeed($server)
    {
        $parameters = [];
        foreach (['database', 'timeout', 'prefix'] as $parameter) {
            if (! empty($server[$parameter])) {
                $parameters[$parameter] = $server[$parameter];
            }
        }
        if (! empty($server['password'])) {
            $parameters['auth'] = $server['password'];
        }
        return $server['host'].':'.$server['port'].'?'.http_build_query($parameters);
    }

    /**
     * Create an array of single connection clients.
     *
     * @param array $server
     * @param array $options
     *
     * @return array
     */
    protected function createSingleClients(array $server, array $options = [])
    {
        $redis = new Redis();
        $timeout = empty($server['timeout']) ? 0 : $server['timeout'];

        if (isset($server['persistent']) && $server['persistent']) {
            $redis->pconnect($server['host'], $server['port'], $timeout);
        } else {
            $redis->connect($server['host'], $server['port'], $timeout);
        }

        if (!empty($server['prefix'])) {
            $redis->setOption(Redis::OPT_PREFIX, $server['prefix']);
        }

        if (!empty($server['password'])) {
            $redis->auth($server['password']);
        }

        if (!empty($server['database'])) {
            $redis->select($server['database']);
        }

        if (!empty($server['serializer'])) {
            $serializer = Redis::SERIALIZER_NONE;
            if ($server['serializer'] === 'php') {
                $serializer = Redis::SERIALIZER_PHP;
            } elseif ($server['serializer'] === 'igbinary') {
                if (defined('Redis::SERIALIZER_IGBINARY')) {
                    $serializer = Redis::SERIALIZER_IGBINARY;
                } else {
                    $serializer = Redis::SERIALIZER_PHP;
                }
            }
            $redis->setOption(Redis::OPT_SERIALIZER, $serializer);
        }

        return $redis;
    }

    /**
     * @param string $name
     * @param bool   $reconnection
     *
     * @return \Redis
     */
    public function connection($name = 'default', $reconnection = false)
    {
        if ($this->cluster) {
            $this->currrent_server = 'default';
            return $this->clients[$this->currrent_server];
        } else {
            if (!$this->clients[$name] || $reconnection) {
                if (!isset($this->servers[$name])) {
                    throw new InvalidArgumentException("Redis server [$name] not found.");
                }
                $this->clients[$name] = $this->createSingleClients($this->servers[$name], $this->options);
            }
            $this->currrent_server = $name;
            return $this;
        }
    }

    /**
     * Reconnection redis, Usually used in the permanent memory task
     *
     * @param \RedisException $e
     *
     * @return bool
     */
    private function isConnectionLost(\RedisException $e)
    {
        if (strpos($e->getMessage(), 'Redis server went away') !== false || strpos($e->getMessage(), 'Connection lost') !== false) {
            return true;
        }

        return false;
    }

    /**
     * Run a command against the Redis database.
     *
     * @param  string $method
     * @param  array  $parameters
     *
     * @return mixed
     */
    public function command($method, array $parameters = [])
    {
        return call_user_func_array([$this->clients[$this->currrent_server], $method], $parameters);
    }

    /**
     * Subscribe to a set of given channels for messages.
     *
     * @param  array|string $channels
     * @param  \Closure     $callback
     * @param  string       $connection
     * @param  string       $method
     *
     * @return void
     */
    public function subscribe($channels, Closure $callback, $connection = null, $method = 'subscribe')
    {
        $loop = $this->connection($connection)->pubSubLoop();

        call_user_func_array([$loop, $method], (array)$channels);

        foreach ($loop as $message) {
            if ($message->kind === 'message' || $message->kind === 'pmessage') {
                call_user_func($callback, $message->payload, $message->channel);
            }
        }

        unset($loop);
    }

    /**
     * Subscribe to a set of given channels with wildcards.
     *
     * @param  array|string $channels
     * @param  \Closure     $callback
     * @param  string       $connection
     *
     * @return void
     */
    public function psubscribe($channels, Closure $callback, $connection = null)
    {
        return $this->subscribe($channels, $callback, $connection, __FUNCTION__);
    }

    /**
     * Dynamically make a Redis command.
     *
     * @param $method
     * @param $parameters
     *
     * @return mixed
     * @throws \RedisException
     */
    public function __call($method, $parameters)
    {
        for ($i = 0; $i < 2; ++$i) {
            try {
                $ret = $this->command($method, $parameters);
            } catch (\RedisException $e) {
                logger()->error('Redis execute error:' . $e->getMessage(), $this->servers[$this->currrent_server]);
                if ($this->isConnectionLost($e)) {
                    $this->connection($this->currrent_server, true);
                    continue;
                } else {
                    throw new \RedisException('Redis execute error:' . $e->getMessage());
                }
            }
            break;
        }

        return $ret;
    }
}