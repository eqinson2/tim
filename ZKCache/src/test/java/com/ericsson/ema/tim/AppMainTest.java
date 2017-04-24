package com.ericsson.ema.tim;

import com.ericsson.ema.tim.zookeeper.ZKMonitor;

import static com.ericsson.ema.tim.zookeeper.ZKConnectionManager.zkConnectionManager;

public class AppMainTest {
    public static void main(String[] args) throws Exception {
        zkConnectionManager.init();
        ZKMonitor zkMonitor = new ZKMonitor(zkConnectionManager);
        zkMonitor.start();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(1000 * 60);
            } catch (InterruptedException e) {
            }
        }

        zkMonitor.stop();
        zkConnectionManager.destroy();
    }
}