package com.inoovalab.c2c.iestorm;

import gate.Gate;
import gate.util.GateException;

import java.io.File;
import java.net.MalformedURLException;

/**
 * Created by rilfi on 3/27/2017.
 */
public class GateMain {
    public static void main(String[] args) {

        try {
            Gate.setGateHome(new File("/opt/gate-8.3-build5704-ALL"));
            Gate.init();
        } catch (GateException e) {
            e.printStackTrace();
        }
        try {
            Gate.getCreoleRegister().registerDirectories(new File(Gate.getPluginsHome(), "ANNIE").toURI().toURL());
        } catch (GateException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }
}
