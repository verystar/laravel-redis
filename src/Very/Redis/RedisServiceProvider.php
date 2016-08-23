<?php

namespace Very\Redis;

use Illuminate\Support\ServiceProvider;

class RedisServiceProvider extends ServiceProvider
{
    protected $defer = true;

    public function register()
    {
        $this->app->singleton('redis', function ($app) {
            return new Database($app['config']['database.redis']);
        });
    }

    public function provides()
    {
        return array('redis');
    }
}
