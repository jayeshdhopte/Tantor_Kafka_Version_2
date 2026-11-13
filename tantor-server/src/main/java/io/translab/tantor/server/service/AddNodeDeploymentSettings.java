package io.translab.tantor.server.service;

import java.nio.file.Path;
import java.util.*;

/** User-editable candidate profile. Cluster identity, version, IDs and listeners remain server-owned. */
public record AddNodeDeploymentSettings(String installPath, String configDirectory, String javaHome,
        String runtimeUser, List<String> brokerDataPaths, String brokerMetadataPath, String brokerLogPath,
        String brokerHeapXmx, List<String> controllerDataPaths, String controllerMetadataPath,
        String controllerLogPath, String controllerHeapXmx) {
    public AddNodeDeploymentSettings {
        brokerDataPaths=brokerDataPaths==null?List.of():List.copyOf(brokerDataPaths);
        controllerDataPaths=controllerDataPaths==null?List.of():List.copyOf(controllerDataPaths);
    }
    public void validate() {
        for(String p:List.of(installPath,configDirectory,brokerLogPath,controllerMetadataPath,controllerLogPath)) approvedPath(p);
        if(brokerMetadataPath==null)throw new IllegalArgumentException("Broker metadata path must be explicit (empty uses the first data directory).");
        if(!brokerMetadataPath.isEmpty())approvedPath(brokerMetadataPath);
        if(!KafkaExpansionPlan.javaHomePath(javaHome))throw new IllegalArgumentException("JAVA_HOME is outside the approved runtime paths.");
        if(runtimeUser==null||!runtimeUser.matches("[a-z_][a-z0-9_-]*[$]?"))throw new IllegalArgumentException("Invalid runtime user.");
        KafkaExpansionPlan.heapBytes(brokerHeapXmx);KafkaExpansionPlan.heapBytes(controllerHeapXmx);
        for(var paths:List.of(brokerDataPaths,controllerDataPaths)) {
            if(paths.isEmpty()||paths.size()>16||new HashSet<>(paths).size()!=paths.size())throw new IllegalArgumentException("Provide 1–16 distinct data paths per process.");
            paths.forEach(AddNodeDeploymentSettings::approvedPath);
        }
        var storage=new ArrayList<>(brokerDataPaths);storage.addAll(controllerDataPaths);
        if(!brokerMetadataPath.isEmpty()&&!brokerDataPaths.contains(brokerMetadataPath))storage.add(brokerMetadataPath);
        if(!controllerDataPaths.contains(controllerMetadataPath))storage.add(controllerMetadataPath);
        for(int i=0;i<storage.size();i++) {
            for(int j=i+1;j<storage.size();j++)if(overlap(storage.get(i),storage.get(j)))throw new IllegalArgumentException("Broker/controller storage paths must be isolated.");
            for(String runtime:List.of(installPath,configDirectory,javaHome,brokerLogPath,controllerLogPath))if(overlap(storage.get(i),runtime))throw new IllegalArgumentException("Storage overlaps configuration, logs or runtime.");
        }
        if(overlap(brokerLogPath,controllerLogPath))throw new IllegalArgumentException("Broker and controller log paths must be isolated.");
        for(String log:List.of(brokerLogPath,controllerLogPath))for(String runtime:List.of(installPath,configDirectory,javaHome))if(overlap(log,runtime))throw new IllegalArgumentException("Logs overlap configuration or runtime.");
        if(overlap(configDirectory,javaHome)||overlap(installPath,javaHome))throw new IllegalArgumentException("Kafka paths overlap JAVA_HOME.");
        for(String dir:List.of("bin","libs"))if(overlap(configDirectory,installPath+"/"+dir))throw new IllegalArgumentException("Configuration overlaps Kafka executables.");
        // A configuration subdirectory of the installation is the normal deployment convention.
        if(overlap(configDirectory,installPath)&&!configDirectory.startsWith(installPath+"/"))throw new IllegalArgumentException("Configuration may be below the installation, never its ancestor.");
    }
    public static void approvedPath(String value) {
        if(value==null||!value.matches("/(?:opt|var|data|srv|app)/[A-Za-z0-9_./-]+")||value.endsWith("/")||!Path.of(value).normalize().toString().replace('\\','/').equals(value))throw new IllegalArgumentException("Path must be canonical and below /opt, /var, /data, /srv or /app: "+value);
    }
    private static boolean overlap(String a,String b){return a.equals(b)||a.startsWith(b+"/")||b.startsWith(a+"/");}
}
