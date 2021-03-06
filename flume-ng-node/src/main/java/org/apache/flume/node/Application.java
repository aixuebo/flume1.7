/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.node;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Channel;
import org.apache.flume.Constants;
import org.apache.flume.Context;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

//应用的入口
public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";//监听类的class全路径
  public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";//监听class需要的配置属性信息前缀

  private final List<LifecycleAware> components;//持有若干个生命周期的对象集合
  private final LifecycleSupervisor supervisor;
  private MaterializedConfiguration materializedConfiguration;
  private MonitorService monitorServer;//创建监听服务

  public Application() {
    this(new ArrayList<LifecycleAware>(0));
  }

  //持有若干个生命周期的对象集合
  public Application(List<LifecycleAware> components) {
    this.components = components;
    supervisor = new LifecycleSupervisor();
  }

  public synchronized void start() {
    for (LifecycleAware component : components) {
      supervisor.supervise(component,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    }
  }

  @Subscribe
  public synchronized void handleConfigurationEvent(MaterializedConfiguration conf) {
    stopAllComponents();
    startAllComponents(conf);
  }

  public synchronized void stop() {
    supervisor.stop();
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }

  private void stopAllComponents() {
    if (this.materializedConfiguration != null) {
      logger.info("Shutting down configuration: {}", this.materializedConfiguration);
      for (Entry<String, SourceRunner> entry :
           this.materializedConfiguration.getSourceRunners().entrySet()) {
        try {
          logger.info("Stopping Source " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, SinkRunner> entry :
           this.materializedConfiguration.getSinkRunners().entrySet()) {
        try {
          logger.info("Stopping Sink " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, Channel> entry :
           this.materializedConfiguration.getChannels().entrySet()) {
        try {
          logger.info("Stopping Channel " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
    }
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }

  //先开启channel、然后开启sink,最后开启source
  private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
    logger.info("Starting new configuration:{}", materializedConfiguration);

    this.materializedConfiguration = materializedConfiguration;

    for (Entry<String, Channel> entry :
        materializedConfiguration.getChannels().entrySet()) {
      try {
        logger.info("Starting Channel " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    /*
     * Wait for all channels to start.
     */
    for (Channel ch : materializedConfiguration.getChannels().values()) {
      while (ch.getLifecycleState() != LifecycleState.START
          && !supervisor.isComponentInErrorState(ch)) {
        try {
          logger.info("Waiting for channel: " + ch.getName() +
              " to start. Sleeping for 500 ms");
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for channel to start.", e);
          Throwables.propagate(e);
        }
      }
    }

    for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
      try {
        logger.info("Starting Sink " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    for (Entry<String, SourceRunner> entry :
         materializedConfiguration.getSourceRunners().entrySet()) {
      try {
        logger.info("Starting Source " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    this.loadMonitoring();
  }

  @SuppressWarnings("unchecked")
  private void loadMonitoring() {
    Properties systemProps = System.getProperties();
    Set<String> keys = systemProps.stringPropertyNames();
    try {
      //解析监听class的全路径
      if (keys.contains(CONF_MONITOR_CLASS)) {
        String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
        Class<? extends MonitorService> klass;
        try {
          //Is it a known type?
          klass = MonitoringType.valueOf(
              monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
        } catch (Exception e) {
          //Not a known type, use FQCN
          klass = (Class<? extends MonitorService>) Class.forName(monitorType);
        }
        this.monitorServer = klass.newInstance();//创建监听对象

        //解析监听class服务需要的配置信息
        Context context = new Context();
        for (String key : keys) {
          if (key.startsWith(CONF_MONITOR_PREFIX)) {
            context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                systemProps.getProperty(key));
          }
        }
        monitorServer.configure(context);
        monitorServer.start();
      }
    } catch (Exception e) {
      logger.warn("Error starting monitoring. "
          + "Monitoring might not be available.", e);
    }

  }

    /**
     * bin/flume-ng agent
     * --conf ./conf/
     * -f conf/flume.conf
     * -Dflume.root.logger=DEBUG,console
     * -n agent1
     */
  public static void main(String[] args) {

    try {

      boolean isZkConfigured = false;//true表示使用zookeeper去加载配置信息

      Options options = new Options();

      Option option = new Option("n", "name", true, "the name of this agent");
      option.setRequired(true);
      options.addOption(option);

      //设置配置文件的绝对路径
      option = new Option("f", "conf-file", true,
          "specify a config file (required if -z missing)");
      option.setRequired(false);
      options.addOption(option);

      //如果设置该参数,表示配置文件更改了也不用重新加载
      option = new Option(null, "no-reload-conf", false,
          "do not reload config file if changed");
      options.addOption(option);

      // Options for Zookeeper 如何连接zookeeper
      option = new Option("z", "zkConnString", true,
          "specify the ZooKeeper connection to use (required if -f missing)");
      option.setRequired(false);
      options.addOption(option);

      //指定zookeeper上配置文件的基础路径
      option = new Option("p", "zkBasePath", true,
          "specify the base path in ZooKeeper for agent configs");
      option.setRequired(false);
      options.addOption(option);

      option = new Option("h", "help", false, "display help text");
      options.addOption(option);

      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args);

      if (commandLine.hasOption('h')) {
        new HelpFormatter().printHelp("flume-ng agent", options, true);
        return;
      }

      String agentName = commandLine.getOptionValue('n');
      boolean reload = !commandLine.hasOption("no-reload-conf");//true表示要动态加载conf配置文件的更改

      if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
        isZkConfigured = true;
      }
      Application application = null;
      if (isZkConfigured) {//使用zookeeper加载配置文件
        // get options
        String zkConnectionStr = commandLine.getOptionValue('z');
        String baseZkPath = commandLine.getOptionValue('p');

        if (reload) {//需要动态加载配置文件的更改
          EventBus eventBus = new EventBus(agentName + "-event-bus");
          List<LifecycleAware> components = Lists.newArrayList();
          PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
              new PollingZooKeeperConfigurationProvider(
                  agentName, zkConnectionStr, baseZkPath, eventBus);
          components.add(zookeeperConfigurationProvider);
          application = new Application(components);
          eventBus.register(application);
        } else {//不需要动态加载配置文件的更改
          StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider = new StaticZooKeeperConfigurationProvider(agentName, zkConnectionStr, baseZkPath);//如何读取静态的zookeeper文件
          application = new Application();
          application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
        }
      } else {//读取配置文件
        File configurationFile = new File(commandLine.getOptionValue('f'));

        /*
         * The following is to ensure that by default the agent will fail on
         * startup if the file does not exist.
         */
        if (!configurationFile.exists()) {
          // If command line invocation, then need to fail fast
          if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
              null) {
            String path = configurationFile.getPath();
            try {
              path = configurationFile.getCanonicalPath();
            } catch (IOException ex) {
              logger.error("Failed to read canonical path for file: " + path,
                  ex);
            }
            throw new ParseException(
                "The specified configuration file does not exist: " + path);
          }
        }
        List<LifecycleAware> components = Lists.newArrayList();

        if (reload) {
          EventBus eventBus = new EventBus(agentName + "-event-bus");
          PollingPropertiesFileConfigurationProvider configurationProvider =
              new PollingPropertiesFileConfigurationProvider(
                  agentName, configurationFile, eventBus, 30);
          components.add(configurationProvider);
          application = new Application(components);
          eventBus.register(application);
        } else {
          PropertiesFileConfigurationProvider configurationProvider =
              new PropertiesFileConfigurationProvider(agentName, configurationFile);
          application = new Application();
          application.handleConfigurationEvent(configurationProvider.getConfiguration());
        }
      }
      application.start();

      final Application appReference = application;
      Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
        @Override
        public void run() {
          appReference.stop();
        }
      });

    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.", e);
    }
  }
}