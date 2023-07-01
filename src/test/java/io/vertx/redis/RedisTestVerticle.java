package io.vertx.redis;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.RedisReplicas;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

public final class RedisTestVerticle extends AbstractVerticle {

  private static final RedisOptions options = new RedisOptions()
    .setType(RedisClientType.CLUSTER)
    .setUseReplicas(RedisReplicas.SHARE)
    // we will flood the redis server
    .setMaxWaitingHandlers(128 * 1024)
    .addConnectionString("redis://127.0.0.1:7000")
    .addConnectionString("redis://127.0.0.1:7001")
    .addConnectionString("redis://127.0.0.1:7002")
    .addConnectionString("redis://127.0.0.1:7003")
    .addConnectionString("redis://127.0.0.1:7004")
    .addConnectionString("redis://127.0.0.1:7005")
    .setMaxPoolSize(8)
    .setMaxPoolWaiting(16);

  public static final GenericContainer<?> redis = new FixedHostPortGenericContainer<>(
    "grokzen/redis-cluster:6.2.0")
    .withEnv("IP", "0.0.0.0")
    .withEnv("STANDALONE", "true")
    .withEnv("SENTINEL", "true")
    .withExposedPorts(7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 5000, 5001, 5002)
    // cluster ports (7000-7005) 6x (master+replica) 3 nodes
    .withFixedExposedPort(7000, 7000)
    .withFixedExposedPort(7001, 7001)
    .withFixedExposedPort(7002, 7002)
    .withFixedExposedPort(7003, 7003)
    .withFixedExposedPort(7004, 7004)
    .withFixedExposedPort(7005, 7005)
    // standalone ports (7006-7007) 2x
    .withFixedExposedPort(7006, 7006)
    .withFixedExposedPort(7007, 7007)
    // sentinel ports (5000-5002) 3x (match the cluster master nodes)
    .withFixedExposedPort(5000, 5000)
    .withFixedExposedPort(5001, 5001)
    .withFixedExposedPort(5002, 5002);

  private static RedisConnection connection;
  private static RedisAPI redisAPI;
  private static final String REDIS_NUMBER_VALUE_KEY = "number_key";
  private static final String REDIS_SET_VALUE_KEY = "set_key";

  public static void main(String[] args) {
    // start redis with docker, must be closed manually in this test, sorry :)
    redis.start();

    Vertx vertx = Vertx.vertx();

    // STEP 1 connect to redis
    connectToRedisCluster(vertx)
      // STEP 2 prepare data
      .compose(placeHolder -> Future.all(addNumberToRedis(), addSetToRedis()))
      .onSuccess(placeHolder -> {
        System.out.println(Thread.currentThread() + "connected to redis! deploy verticles");
        // STEP 3 start parallel schedule tasks in verticle
        //        redis response will be in a mass with multiple verticles and high load
        vertx.deployVerticle(RedisTestVerticle.class,
          new DeploymentOptions().setInstances(2));
      })
      .onFailure(ex -> {
        System.out.println(Thread.currentThread() + "ex msg: " + ex.getMessage());
        ex.printStackTrace();
      });
  }

  private static Future<Void> addNumberToRedis() {
    List<String> params = new ArrayList<>();
    params.add(REDIS_NUMBER_VALUE_KEY);
    params.add("42");
    return redisAPI.set(params).mapEmpty();
  }

  private static Future<Void> addSetToRedis() {
    List<String> params = new ArrayList<>();
    params.add(REDIS_SET_VALUE_KEY);
    params.add("100");
    params.add("101");
    params.add("102");
    return redisAPI.sadd(params).mapEmpty();
  }

  private static Future<Void> connectToRedisCluster(Vertx vertx) {
    return Redis.createClient(vertx, options)
      .connect()
      .compose(conn -> {
        // use the connection
        connection = conn;
        redisAPI = RedisAPI.api(connection);
        return Future.succeededFuture();
      }).mapEmpty();
  }

  @Override
  public void start(Promise<Void> startPromise) {
    startPromise.complete();
    vertx.setTimer(1000, timerId -> {
      loopTest();
    });
  }

  public static void loopTest() {
    for (int i = 0; i < 1000; i++) {
      System.out.println(Thread.currentThread() + "execute loop: " + i);
      test1();
    }
  }

  public static void test1() {
    Future<Integer> fetchNumberFuture = redisAPI.get(REDIS_NUMBER_VALUE_KEY)
      .map(response -> response.toInteger());

    Future<Set<Integer>> fetchSetFuture = redisAPI.smembers(REDIS_SET_VALUE_KEY)
      .map(response -> response.stream().map(res -> res.toInteger()).collect(Collectors.toSet()));

    Future.all(fetchNumberFuture, fetchSetFuture)
      .onSuccess(compositeRet -> {
        Integer number = fetchNumberFuture.result();
        Set<Integer> set = fetchSetFuture.result();
        //System.out.println(Thread.currentThread() + "number is: " + number + ", set is: " + set);
      })
      .onFailure(ex -> {
        System.out.println(Thread.currentThread() + "ex msg: " + ex.getMessage());
        ex.printStackTrace();
      });
  }

//  public static Future test() {
//
//    // 猜一下 是不是 String.format线程问题
//    String key1 = String.format("user:like:post:%s", 975);
//    Future f1 = RedisUtils.redisApi.smembers(key1).map(r -> {
//      Set<Long> ret = r.stream().map(Response::toLong).collect(Collectors.toSet());
//      return Future.succeededFuture(ret);
//    });
//    Future f2 = RedisUtils.redisApi.get(String.format("user:post:pinned:%s", 1372)).map(r -> {
//      return r.toInteger();
//    });
//    return CompositeFuture.all(f1, f2).compose(result -> {
//      f1.result();
//      f2.result();
//      return Future.succeededFuture();
//    }).onFailure(e -> {
//      System.out.println(Thread.currentThread().getName() +
//          ": Hello Verticle : " +
//          Thread.currentThread().getId() + " " + e.getMessage());
//    });
//  }

}
