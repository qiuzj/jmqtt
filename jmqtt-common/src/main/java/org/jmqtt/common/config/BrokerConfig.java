package org.jmqtt.common.config;

/**
 * Broker配置，即服务端的配置.
 */
public class BrokerConfig {
    /** JMQTT的主目录，即jmqtt-distribution目录路径. 通过Java系统变量jmqttHome获取，否则通过操作系统环境变量JMQTT_HOME获取 */
    private String jmqttHome = System.getProperty("jmqttHome", System.getenv("JMQTT_HOME"));

    private String version = "1.0.0";

    private boolean anonymousEnable = true;

    private int pollThreadNum = Runtime.getRuntime().availableProcessors() * 2;

    public int getPollThreadNum() {
        return pollThreadNum;
    }

    public void setPollThreadNum(int pollThreadNum) {
        this.pollThreadNum = pollThreadNum;
    }

    public String getJmqttHome() {
        return jmqttHome;
    }

    public void setJmqttHome(String jmqttHome) {
        this.jmqttHome = jmqttHome;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isAnonymousEnable() {
        return anonymousEnable;
    }

    public void setAnonymousEnable(boolean anonymousEnable) {
        this.anonymousEnable = anonymousEnable;
    }
}
