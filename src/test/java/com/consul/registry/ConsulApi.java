package com.consul.registry;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhujianxin
 * @date 2019/1/16.
 */
public class ConsulApi {


    public static void main(String[] args) {

        Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts("192.168.10.25",8500))
                .build();
        Map<String,ServiceHealthCache> serviceHealthCacheMap = new HashMap<>();
        while (true){
            if(!serviceHealthCacheMap.containsKey("ym-ads-adapter")){
                System.out.println("addddddddddddd");
                ServiceHealthCache healthCache = ServiceHealthCache.newCache(client.healthClient(),"ym-ads-adapter");
                healthCache.addListener((Map<ServiceHealthKey, ServiceHealth> map) -> {
                    map.keySet().forEach(serviceHealthKey -> {
                        System.out.println("serviceHealthKey:" + serviceHealthKey.getServiceId());
                        map.get(serviceHealthKey).getChecks().forEach(healthCheck -> {
                            System.out.println("health:" + healthCheck.getServiceId().get() + "_" + healthCheck.getStatus());
                        });
                    });
                });
                healthCache.start();
                serviceHealthCacheMap.put("ym-ads-adapter",healthCache);
            }
        }

    }
}
