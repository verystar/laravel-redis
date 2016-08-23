PhpRedis Connector for Laravel
==============================

Use phpredis as the redis connection in Laravel, support retry connection
Requirements
------------

 - PHP 5.3+
 - [PhpRedis]
 - Laravel 5.2.x

Installation
-------------

First, of course, make sure PhpRedis is installed on the server. See [here][1] for installation instructions.

Add the dependency to `composer.json`:

```
"require": {
    "very/laravel-phpredis": "1.*"
}
```

Add the `RedisServiceProvider` to `config/app.php` (comment out built-in `RedisServiceProvider`):

```
...
'providers' => array(
    ...
    // 'Illuminate\Redis\RedisServiceProvider',
    'Very\Redis\RedisServiceProvider',
    ...
),
...
```

The default Facade alias conflicts with the Redis class provided by PhpRedis.
To fix this, rename the alias in `config/app.php`:

```
...
'aliases' => array(
    ...
    'Redis'           => 'Illuminate\Support\Facades\Redis', 
    ...
),
...
```

An unfortunate side effect is that you need to call the Redis functions like `Redis::connection()` now which does not look as nice or slick, but everything still works the same way (you can call Redis commands as usual, e.g. `Redis::get('key')`.

Finally run `composer update` to update and install everything.

Options
-------

Configuration is just like the default config for Redis in Laravel. In fact, you can switch between PhpRedis and Predis without changing your configuration (no guarantees for clustering or serialization though).

All options are optional, you can specify an empty array to get the default connection configuration:

```
'redis' => array(

    'cluster' => true, // if true a RedisArray will be created

    'default' => array(
        'host'       => '127.0.0.1', // default: '127.0.0.1'
        'port'       => 6379,        // default: 6379
        'password'   => password     // default: null
        'prefix'     => 'myapp:',    // default: ''
        'database'   => 7,           // default: 0
        'timeout'    => 0.5,         // default: 0 (no timeout)
        'serializer' => 'igbinary'   // default: 'none', possible values: 'none', 'php', 'igbinary'
    ),

),
```

The only option that is not self-explanatory is the `serializer` option. The values correspond directly to the `Redis::SERIALIZER_*` constants in PhpRedis. If you specify `igbinary`, igbinary will be used as the serializer if PhpRedis was compiled with `--enable-redis-igbinary`, falling back to PHP's built-in serializer otherwise. 

[laravel]:http://laravel.com/
[phpredis]:https://github.com/nicolasff/phpredis
[1]:https://github.com/nicolasff/phpredis#installingconfiguring
