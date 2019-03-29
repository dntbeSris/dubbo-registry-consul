package com.dubbo.registry.consul;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.alibaba.dubbo.common.Constants.*;

/**
 * @author zhujianxin
 * @date 2019/1/16.
 */
public class ConsulRegistry extends FailbackRegistry {

    private static final List<String> ALL_SUPPORTED_CATEGORIES = new ArrayList<String>(4){{
        add(PROVIDERS_CATEGORY);
        add(CONSUMERS_CATEGORY);
        add(ROUTERS_CATEGORY);
        add(CONFIGURATORS_CATEGORY);
    }};

    private static final String CONSUL_SERVICE_ID_PREFIX = "dubbo_consul_registry";

    private static final Long CONSUL_HEALTH_CHECK_INTERVAL = 5L;

    private static final Long CONSUL_HEALTH_CHECK_TIMEOUT = 1000L;

    private static final ExecutorService SERVICE_SCHEDULER = Executors.newSingleThreadScheduledExecutor((r) -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("CONSUL_SERVICE_THREAD");
        return thread;
    });

    private final AgentClient consulAgentClient;

    private final HealthClient consulHealthClient;

    private final Map<String, ServiceHealthCache> serviceHealthCacheMap = new ConcurrentHashMap<>(16);

    public ConsulRegistry(URL url, Consul consul) {
        super(url);
        this.consulAgentClient = consul.agentClient();
        this.consulHealthClient = consul.healthClient();
    }


    @Override
    public boolean isAvailable() {

        List<Service> healthDubboService = consulAgentClient.getServices().values().stream().filter(service -> {
            boolean isDubboService = service.getId().startsWith(CONSUL_SERVICE_ID_PREFIX);
            if (isDubboService) {
                HealthCheck healthCheck = consulAgentClient.getChecks().get(service.getService());
                if (healthCheck.getStatus().equals(State.PASS.name())) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());

        return CollectionUtils.isNotEmpty(healthDubboService);
    }

    @Override
    public void destroy() {

        if (serviceHealthCacheMap != null && CollectionUtils.isNotEmpty(serviceHealthCacheMap.values())) {
            serviceHealthCacheMap.values().forEach(serviceHealthCache -> {
                serviceHealthCache.close();
            });
        }

        //unregistry consul service and shutdown some process
    }


    /**
     * 注册服务.
     * <p>
     * 注册需处理契约：<br>
     * 1. 当URL设置了check=false时，注册失败后不报错，在后台定时重试，否则抛出异常。<br>
     * 2. 当URL设置了dynamic=false参数，则需持久存储，否则，当注册者出现断电等情况异常退出时，需自动删除。<br>
     * 3. 当URL设置了category=overrides时，表示分类存储，缺省类别为providers，可按分类部分通知数据。<br>
     * 4. 当注册中心重启，网络抖动，不能丢失数据，包括断线自动删除数据。<br>
     * 5. 允许URI相同但参数不同的URL并存，不能覆盖。<br>
     *
     * @param url 注册信息，不允许为空，如：dubbo://192.168.26.113:8888/com.dubbo.api.service.SafeCheckService?anyhost=true&application=dubbo-provider&application.version=1.0.0&bean.name=ServiceBean:com.dubbo.api.service.SafeCheckService:1.0.0&dubbo=2.0.2&generic=false&interface=com.dubbo.api.service.SafeCheckService&methods=isSafe&pid=31583&revision=1.0.0&side=provider&status=server&timestamp=1553763998629&version=1.0.0
     */
    @Override
    protected void doRegister(URL url) {
        consulAgentClient.register(buildRegistration(url));
    }

    /**
     * 取消注册服务.
     * <p>
     * 取消注册需处理契约：<br>
     * 1. 如果是dynamic=false的持久存储数据，找不到注册数据，则抛IllegalStateException，否则忽略。<br>
     * 2. 按全URL匹配取消注册。<br>
     *
     * @param url 注册信息，不允许为空，如：dubbo://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     */
    @Override
    protected void doUnregister(URL url) {
        consulAgentClient.deregister(url2ServiceId(url));
    }

    /**
     * 订阅服务.
     * <p>
     * 订阅需处理契约：<br>
     * 1. 当URL设置了check=false时，订阅失败后不报错，在后台定时重试。<br>
     * 2. 当URL设置了category=overrides，只通知指定分类的数据，多个分类用逗号分隔，并允许星号通配，表示订阅所有分类数据。<br>
     * 3. 允许以interface,group,version,classifier作为条件查询，如：interface=com.alibaba.foo.BarService&version=1.0.0<br>
     * 4. 并且查询条件允许星号通配，订阅所有接口的所有分组的所有版本，或：interface=*&group=*&version=*&classifier=*<br>
     * 5. 当注册中心重启，网络抖动，需自动恢复订阅请求。<br>
     * 6. 允许URI相同但参数不同的URL并存，不能覆盖。<br>
     * 7. 必须阻塞订阅过程，等第一次通知完后再返回。<br>
     *
     * @param url            订阅条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param notifyListener 变更事件监听器，不允许为空
     */
    @Override
    protected void doSubscribe(URL url, NotifyListener notifyListener) {

        List<String> dubboServiceNames = getAllDubboServiceNames();
        //1. getServiceNames

        if (!serviceHealthCacheMap.containsKey(serviceName)) {
            ServiceHealthCache serviceHealthCache = ServiceHealthCache.newCache(consulHealthClient, serviceName);
            ConsulCache.Listener<ServiceHealthKey, ServiceHealth> listener = map -> {
                final List<ServiceHealth> healthServiceList = map.values()
                        .stream()
                        .filter(healthService -> {
                            List<HealthCheck> healthChecks = healthService.getChecks();

                            boolean isPassing = false;

                            for (HealthCheck healthCheck : healthChecks) {
                                if (healthCheck.getServiceId().get().equals(healthService.getService().getId())
                                        && healthCheck.getStatus().equals(State.PASS.name())) {
                                    isPassing = true;
                                }
                            }
                            return isPassing;

                        }).collect(Collectors.toList());
                // Healthy Service
                List<URL> urls = buildURLs(url, healthServiceList);
                ConsulRegistry.this.notify(url, notifyListener, urls);
            };
            serviceHealthCache.addListener(listener);
            serviceHealthCacheMap.putIfAbsent(serviceName, serviceHealthCache);
            serviceHealthCache.start();
        }

    }

    private List<String> getAllDubboServiceNames() {
        return consulAgentClient.getServices()
                .values().stream()
                .filter(service -> {
                    String servicePrefix = service.getService().substring(0,service.getService().indexOf(':'));
                    if(ALL_SUPPORTED_CATEGORIES.contains(servicePrefix)){
                        return true;
                    }
                    return false;
                })
                .map(service -> service.getService())
                .collect(Collectors.toList());
    }

    /**
     * 取消订阅服务.
     * <p>
     * 取消订阅需处理契约：<br>
     * 1. 如果没有订阅，直接忽略。<br>
     * 2. 按全URL匹配取消订阅。<br>
     *
     * @param url            订阅条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param notifyListener 变更事件监听器，不允许为空
     */
    @Override
    protected void doUnsubscribe(URL url, NotifyListener notifyListener) {
        SERVICE_SCHEDULER.shutdown();
    }


    /**
     * 查询注册列表，与订阅的推模式相对应，这里为拉模式，只返回一次结果。
     *
     * @param url 查询条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @return 已注册信息列表，可能为空，含义同{@link org.apache.dubbo.registry.NotifyListener#notify(List<URL>)}的参数。
     * @see org.apache.dubbo.registry.NotifyListener#notify(List)
     */
    @Override
    public List<URL> lookup(final URL url) {
        final List<URL> matchedURLs = new ArrayList<>();
        consulAgentClient.getServices().values().stream().filter(service -> {
            boolean isDubboService = service.getId().startsWith(CONSUL_SERVICE_ID_PREFIX);
            if (isDubboService) {
                HealthCheck healthCheck = consulAgentClient.getChecks().get(service.getService());
                if (healthCheck.getStatus().equals(State.PASS.name())) {
                    return true;
                }
            }
            return false;
        }).forEach(service -> {
            URL matchURL = service2URL(service);
            if (UrlUtils.isMatch(url, matchURL)) {
                matchedURLs.add(matchURL);
            }
        });
        return matchedURLs;
    }

    /**
     * 生成consul serviceId,consul服务注册需要，且唯一
     * 生成规则: dubbo_consul_registry_localip,ex:dubbo_consul_registry_ip_inteface
     *
     * @param url
     */
    private String url2ServiceId(URL url) {
        String ip = NetUtils.getLocalHost();
        String itfc = url.getParameter(Constants.INTERFACE_KEY);
        return CONSUL_SERVICE_ID_PREFIX + "_" + ip + "_" + itfc;
    }

    /**
     * 创建consul注册信息
     * name:catagory:interface:version
     * consul中，serviceId需要唯一，规则为:dubbo_consul_registry_ip_port
     *
     * @param url
     */
    private Registration buildRegistration(URL url) {

        String address = url.getAddress();
        int port = url.getPort();

        if (StringUtils.isBlank(address)) {
            throw new IllegalArgumentException("address is null");
        }

        Registration.RegCheck regCheck = Registration.RegCheck.tcp(address, CONSUL_HEALTH_CHECK_INTERVAL, CONSUL_HEALTH_CHECK_TIMEOUT);

        ImmutableRegistration.Builder builder = ImmutableRegistration.builder();

        builder.name(getServiceName(url))
                .id(url2ServiceId(url))
                .address(address)
                .port(port)
                .check(regCheck);

        if (url.getParameters() != null) {
            builder.meta(url.getParameters());
        }
        List<String> tags = new ArrayList<String>() {{
            add(CONSUL_SERVICE_ID_PREFIX);
        }};

        builder.tags(tags);

        return builder.build();
    }


    private List<URL> buildURLs(URL url, List<ServiceHealth> healthServiceList) {
        final List<URL> urls = new ArrayList<>();
        healthServiceList.forEach(serviceHealth -> {
            Service service = serviceHealth.getService();
            urls.add(service2URL(service));
        });
        return urls;
    }

    private String getServiceName(URL url) {
        StringBuilder serviceName = new StringBuilder();

        serviceName.append(url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY)).append(":")
                .append(url.getParameter(Constants.INTERFACE_KEY)).append(":")
                .append(url.getParameter(Constants.VERSION_KEY));
        return serviceName.toString();
    }

    private URL service2URL(Service service) {
        URL url = new URL(service.getMeta().get(Constants.PROTOCOL_KEY),
                service.getAddress(),
                service.getPort(),
                service.getMeta());
        return url;
    }
}
