package com.dubbo.registry.consul;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.KVCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.health.ServiceHealth;

import java.util.Map;
import java.util.Properties;

import static com.alibaba.dubbo.common.Constants.BACKUP_KEY;

/**
 * @author zhujianxin
 * @date 2019/1/16.
 */
public class ConsulRegistryFactory implements RegistryFactory {

    @Override
    public Registry getRegistry(URL url) {
        return new ConsulRegistry(url,Consul.builder().withHostAndPort(HostAndPort.fromParts(url.getHost(),url.getPort())).build());
    }

}
