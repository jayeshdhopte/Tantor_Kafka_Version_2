package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.*;

/** Backend/native wire contract. Only server-collected evidence can freeze this plan. */
public record KafkaExpansionPlan(
        String schemaVersion, UUID operationId, UUID clusterId, String contextHash,
        String candidateHostId, String candidateIp, String kafkaClusterId, String kafkaVersion,
        KafkaExpansionWorkflow.Mode quorumMode, KafkaExpansionWorkflow.Layout layout,
        Map<String,Short> features, UUID artifactId, String artifactSha256,
        String parcelDir, String javaHome, String runtimeUser, List<Process> processes,
        List<Participant> participants, List<Voter> voters, List<String> excludedHostIds,
        List<String> excludedHostIps, boolean maintenanceAcknowledged, ResourcePolicy resourcePolicy, @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) AddNodeDeploymentSettings deploymentSettings) {
    public KafkaExpansionPlan(
        String schemaVersion, UUID operationId, UUID clusterId, String contextHash,
        String candidateHostId, String candidateIp, String kafkaClusterId, String kafkaVersion,
        KafkaExpansionWorkflow.Mode quorumMode, KafkaExpansionWorkflow.Layout layout,
        Map<String,Short> features, UUID artifactId, String artifactSha256,
        String parcelDir, String javaHome, String runtimeUser, List<Process> processes,
        List<Participant> participants, List<Voter> voters, List<String> excludedHostIds,
        List<String> excludedHostIps, boolean maintenanceAcknowledged, ResourcePolicy resourcePolicy){this(schemaVersion,operationId,clusterId,contextHash,candidateHostId,candidateIp,kafkaClusterId,kafkaVersion,quorumMode,layout,features,artifactId,artifactSha256,parcelDir,javaHome,runtimeUser,processes,participants,voters,excludedHostIds,excludedHostIps,maintenanceAcknowledged,resourcePolicy,null);}

    public record Runtime(UUID jmxArtifactId,String jmxChecksum,int jmxPort,int limitNofile) {}
    public record Process(String role,int nodeId,int port,List<String> dataPaths,String metadataPath,
                          String logPath,String heapXmx,String brokerRack,Map<String,String> inherited,Runtime runtime) {
        public Process {dataPaths=List.copyOf(dataPaths);inherited=Map.copyOf(inherited);}
    }
    public record Storage(String path,String directoryId) {}
    public record Participant(List<UUID> serviceIds,String hostId,String hostIp,String role,int nodeId,
                              String unit,String configPath,String configSha256,String unitSha256,
                              String artifactProofSha256,String runtimeHome,Map<String,String> javaAgents,Map<String,String> resolvedPaths,
                              List<Storage> storage,int pid,String startTicks,int controllerPort) {
        public Participant {serviceIds=List.copyOf(serviceIds);storage=List.copyOf(storage);javaAgents=Map.copyOf(javaAgents);resolvedPaths=resolvedPaths==null?Map.of():Map.copyOf(resolvedPaths);}
        public String key(){return hostId+":"+unit;}
    }
    public record Voter(int nodeId,String directoryId,String hostIp,int port) {}
    public record ResourcePolicy(long minAvailableMemoryMb,long osReserveMb,long minDiskGb,long minInodes,List<Integer> allowedJavaMajors) {
        public ResourcePolicy {allowedJavaMajors=List.copyOf(allowedJavaMajors);}
    }
    public record Frozen(KafkaExpansionPlan plan,String raw,String digest,List<KafkaExpansionWorkflow.Step> steps) {}
    public KafkaExpansionPlan {
        features=Map.copyOf(features);processes=List.copyOf(processes);participants=List.copyOf(participants);
        voters=List.copyOf(voters);excludedHostIds=List.copyOf(excludedHostIds);excludedHostIps=List.copyOf(excludedHostIps);
    }
    public Process process(String role){return processes.stream().filter(p->p.role().equals(role)).findFirst().orElseThrow(()->invalid("Missing candidate role."));}
    public String configPath(String role){process(role);return deploymentSettings!=null?deploymentSettings.configDirectory()+"/"+role+".properties":"/etc/tantor-kafka/add-expansion/"+operationId+"/"+role+".properties";}
    public String unit(String role){return deploymentSettings!=null?role+".service":"tantor-kafka-"+role+"-"+process(role).nodeId()+".service";}
    public List<String> reservedHosts(){var hosts=new TreeSet<String>();hosts.add(candidateHostId);participants.forEach(p->hosts.add(p.hostId()));return List.copyOf(hosts);}
    public List<KafkaExpansionWorkflow.Step> steps(){return KafkaExpansionWorkflow.compile(quorumMode,layout,participants.stream().map(p->new KafkaExpansionWorkflow.Participant(p.hostId(),p.unit(),p.nodeId(),p.role())).toList());}
    public String voterDigest(){return BrokerAdditionLedger.hash(voters.stream().map(v->v.nodeId()+":"+v.directoryId()+"@"+endpoint(v.hostIp(),v.port())).sorted().collect(java.util.stream.Collectors.joining(",")));}
    public Frozen freeze(ObjectMapper mapper){validate();try{String raw=mapper.writeValueAsString(this);if(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>2*1024*1024)throw invalid("Expansion plan is too large.");return new Frozen(this,raw,BrokerAdditionLedger.hash(raw),steps());}catch(com.fasterxml.jackson.core.JsonProcessingException e){throw new IllegalStateException("Cannot freeze expansion plan.",e);}}
    public static Frozen read(ObjectMapper mapper,String raw,String digest){
        if(raw==null||raw.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>2*1024*1024||!hash(digest)||!BrokerAdditionLedger.hash(raw).equals(digest))throw invalid("Frozen expansion digest changed.");
        try{var plan=mapper.readValue(raw,KafkaExpansionPlan.class);plan.validate();return new Frozen(plan,raw,digest,plan.steps());}catch(com.fasterxml.jackson.core.JsonProcessingException e){throw invalid("Invalid frozen expansion plan.");}
    }
    public void validate(){
        if("add-quorum-expansion/v2".equals(schemaVersion)){if(deploymentSettings==null)throw invalid("Explicit deployment settings required.");deploymentSettings.validate();}else if(deploymentSettings!=null)throw invalid("Legacy expansion cannot carry deployment settings.");
        if(!("add-quorum-expansion/v1".equals(schemaVersion)||"add-quorum-expansion/v2".equals(schemaVersion))||operationId==null||clusterId==null||artifactId==null||!hash(contextHash)||!hash(artifactSha256)||candidateHostId==null||candidateHostId.isBlank()||!ip(candidateIp)||!kafkaId(kafkaClusterId)||!Set.of("4.1.0","4.1.1","4.1.2","4.3.0").contains(kafkaVersion)||quorumMode==null||layout==null||runtimeUser==null||!runtimeUser.matches("[a-z_][a-z0-9_-]*[$]?"))throw invalid("Incomplete expansion identity, version or catalog artifact.");
        if(deploymentSettings!=null&&(overlap(deploymentSettings.configDirectory(),parcelDir)||overlap(deploymentSettings.installPath(),parcelDir)))throw invalid("Candidate configuration/installation overlaps cached Kafka parcels.");
        if(!path(parcelDir))throw invalid("Candidate Kafka parcel directory is outside the approved runtime roots.");
        if(!javaHomePath(javaHome))throw invalid("Candidate JAVA_HOME must be an approved runtime path or a JDK directory under /usr/lib/jvm.");
        if(excluded(candidateHostId,candidateIp))throw invalid("Candidate host is excluded from Add Node execution.");
        if(features.getOrDefault("metadata.version",(short)0)<1)throw invalid("A finalized metadata.version is required for expansion.");
        if(quorumMode==KafkaExpansionWorkflow.Mode.DYNAMIC&&features.getOrDefault("kraft.version",(short)0)<1||quorumMode==KafkaExpansionWorkflow.Mode.STATIC&&features.getOrDefault("kraft.version",(short)0)!=0)throw invalid("Expansion quorum mode differs from the live finalized kraft.version.");
        if(quorumMode==KafkaExpansionWorkflow.Mode.STATIC&&!maintenanceAcknowledged)throw invalid("Static expansion requires explicit maintenance acknowledgement.");
        if(resourcePolicy==null||resourcePolicy.minAvailableMemoryMb()<1||resourcePolicy.minAvailableMemoryMb()>1048576||resourcePolicy.osReserveMb()<1||resourcePolicy.osReserveMb()>1048576||resourcePolicy.minDiskGb()<1||resourcePolicy.minDiskGb()>1048576||resourcePolicy.minInodes()<1||resourcePolicy.minInodes()>1000000000||resourcePolicy.allowedJavaMajors().isEmpty()||resourcePolicy.allowedJavaMajors().size()>8||resourcePolicy.allowedJavaMajors().stream().anyMatch(v->v==null||v<17||v>99))throw invalid("Explicit bounded resource policy required.");
        if(participants.isEmpty()||participants.size()>128||voters.isEmpty()||voters.size()>128||processes.size()!=(layout==KafkaExpansionWorkflow.Layout.SEPARATE?2:1))throw invalid("Invalid expansion scope.");
        var ids=new HashSet<Integer>();var units=new HashSet<String>();var services=new HashSet<UUID>();var directories=new HashSet<String>();var byId=new HashMap<Integer,Participant>();var hosts=new HashMap<String,String>();int maximum=100,maximumBroker=0;boolean broker=false;
        for(var p:participants){
            if(p.hostId()==null||p.hostId().isBlank()||candidateHostId.equals(p.hostId())||!ip(p.hostIp())||sameIp(p.hostIp(),candidateIp)||excluded(p.hostId(),p.hostIp())||!Set.of("broker","controller","broker,controller").contains(p.role())||p.nodeId()<0||!ids.add(p.nodeId())||!unitName(p.unit())||!units.add(p.key())||!config(p.configPath())||!hash(p.configSha256())||!hash(p.unitSha256())||!hash(p.artifactProofSha256())||!path(p.runtimeHome())||p.pid()<1||p.startTicks()==null||!p.startTicks().matches("[1-9][0-9]*"))throw invalid("Unverified or duplicate participant identity.");
            String canonical=canonicalIp(p.hostIp());String owner=hosts.putIfAbsent(canonical,p.hostId());if(owner!=null&&!owner.equals(p.hostId()))throw invalid("Ambiguous participant host alias.");
            if(p.serviceIds().isEmpty()||p.serviceIds().size()>2||p.serviceIds().stream().anyMatch(id->id==null||!services.add(id))||p.storage().isEmpty()||p.storage().size()>17||p.javaAgents().size()>8)throw invalid("Incomplete participant service/storage ownership.");
            var paths=new HashSet<String>();for(var s:p.storage())if(!path(s.path())||!paths.add(s.path())||!nonzeroKafkaId(s.directoryId())||!directories.add(s.directoryId()))throw invalid("Copied or ambiguous participant storage identity.");
            for(var agent:p.javaAgents().entrySet())if(!path(agent.getKey())||!hash(agent.getValue()))throw invalid("Unapproved participant Java agent.");
            if(p.resolvedPaths().size()>32)throw invalid("Too many participant path bindings.");for(var binding:p.resolvedPaths().entrySet())if(!binding(p,binding.getKey(),binding.getValue()))throw invalid("Participant path binding is outside the frozen file scope.");
            if(p.role().contains("controller")&&!port(p.controllerPort()))throw invalid("Observed controller listener required.");
            maximum=Math.max(maximum,p.nodeId());if(p.role().contains("broker")){broker=true;maximumBroker=Math.max(maximumBroker,p.nodeId());}byId.put(p.nodeId(),p);
        }
        if(!broker)throw invalid("Existing broker membership is required.");
        var voterIds=new HashSet<Integer>();var endpoints=new HashSet<String>();for(var v:voters){var p=byId.get(v.nodeId());if(p==null||!p.role().contains("controller")||!p.hostIp().equals(v.hostIp())||p.controllerPort()!=v.port()||!kafkaId(v.directoryId())||!voterIds.add(v.nodeId())||!endpoints.add(endpoint(v.hostIp(),v.port())))throw invalid("Voter baseline differs from participant evidence.");if(quorumMode==KafkaExpansionWorkflow.Mode.DYNAMIC&&(!nonzeroKafkaId(v.directoryId())||p.storage().stream().noneMatch(s->s.directoryId().equals(v.directoryId()))))throw invalid("Dynamic voter directory lacks matching native storage proof.");}
        for(var p:participants)if(p.role().contains("controller")&&!voterIds.contains(p.nodeId()))throw invalid("Unexplained nonvoting controller.");
        var ports=new HashSet<Integer>();var mutablePaths=new ArrayList<String>();var logs=new ArrayList<String>();
        for(int i=0;i<processes.size();i++){var p=processes.get(i);String role=i==0?"controller":"broker";long next=(long)(i==0?maximum:maximumBroker)+1;
            if(!role.equals(p.role())||p.nodeId()!=next||next>Integer.MAX_VALUE||!ids.add(p.nodeId())||!port(p.port())||!ports.add(p.port())||p.runtime()==null||p.runtime().limitNofile()<1024||p.dataPaths().isEmpty()||p.dataPaths().size()>16||!path(p.logPath())||role.equals("controller")&&(p.metadataPath()==null||p.metadataPath().isEmpty()))throw invalid("Invalid candidate role, sequential ID or isolated runtime.");
            if(deploymentSettings!=null){var s=deploymentSettings;boolean controller=role.equals("controller");if(!p.dataPaths().equals(controller?s.controllerDataPaths():s.brokerDataPaths())||!Objects.equals(p.metadataPath(),controller?s.controllerMetadataPath():s.brokerMetadataPath())||!p.logPath().equals(controller?s.controllerLogPath():s.brokerLogPath())||!p.heapXmx().equals(controller?s.controllerHeapXmx():s.brokerHeapXmx())||!javaHome.equals(s.javaHome())||!runtimeUser.equals(s.runtimeUser()))throw invalid("Process differs from frozen candidate deployment settings.");}
            heapBytes(p.heapXmx());var own=new HashSet<String>();for(String value:p.dataPaths())if(!path(value)||!own.add(value))throw invalid("Duplicate candidate storage.");if(p.metadataPath()!=null&&!p.metadataPath().isEmpty()){if(!path(p.metadataPath()))throw invalid("Invalid metadata path.");own.add(p.metadataPath());}mutablePaths.addAll(own);logs.add(p.logPath());
            if(p.brokerRack()==null||!p.brokerRack().matches("[A-Za-z0-9_.-]{0,128}")||role.equals("controller")&&!p.brokerRack().isEmpty())throw invalid("Invalid candidate rack.");
            var runtime=p.runtime();if(runtime.jmxPort()!=0&&(!port(runtime.jmxPort())||!ports.add(runtime.jmxPort())||runtime.jmxArtifactId()==null||!hash(runtime.jmxChecksum()))||runtime.jmxPort()==0&&(runtime.jmxArtifactId()!=null||runtime.jmxChecksum()!=null&&!runtime.jmxChecksum().isEmpty()))throw invalid("Candidate JMX ports/artifacts are ambiguous.");
            for(var property:p.inherited().entrySet())if(!safeInherited(property.getKey(),property.getValue()))throw invalid("Unsupported inherited property adapter.");
        }
        for(int i=0;i<mutablePaths.size();i++){String a=mutablePaths.get(i);for(int j=i+1;j<mutablePaths.size();j++)if(overlap(a,mutablePaths.get(j)))throw invalid("Candidate storage overlaps.");for(String b:logs)if(overlap(a,b))throw invalid("Candidate storage overlaps logs.");if(overlap(a,parcelDir)||overlap(a,javaHome))throw invalid("Candidate storage overlaps immutable runtime.");}
        for(int i=0;i<logs.size();i++){if(overlap(logs.get(i),parcelDir)||overlap(logs.get(i),javaHome))throw invalid("Candidate logs overlap runtime.");for(int j=i+1;j<logs.size();j++)if(overlap(logs.get(i),logs.get(j)))throw invalid("Candidate logs overlap.");}
    }
    private boolean excluded(String host,String address){return excludedHostIds.contains(host)||excludedHostIps.stream().anyMatch(ip->sameIp(ip,address));}
    private static boolean safeInherited(String key,String value){if(key==null||value==null||!key.matches("[A-Za-z][A-Za-z0-9._-]{0,127}")||!value.equals(value.strip())||value.matches("(?s).*[\\r\\n\\x00\\\\].*"))return false;String lower=key.toLowerCase(Locale.ROOT);if(List.of("password","jaas","secret","credential","token","keystore","truststore","sasl","ssl","authorizer").stream().anyMatch(lower::contains))return false;return !Set.of("cluster.id","node.id","broker.id","process.roles","listeners","advertised.listeners","log.dir","log.dirs","metadata.log.dir","controller.quorum.voters","controller.quorum.bootstrap.servers","controller.quorum.auto.join.enable","controller.listener.names","listener.security.protocol.map","inter.broker.listener.name","broker.rack","zookeeper.connect","super.users","security.protocol").contains(lower);}
    public static long heapBytes(String value){if(value==null||!value.matches("[1-9][0-9]*[mMgG]"))throw invalid("Explicit bounded JVM heap required.");try{return Math.multiplyExact(Long.parseLong(value.substring(0,value.length()-1)),value.toLowerCase(Locale.ROOT).endsWith("g")?1024L*1024*1024:1024L*1024);}catch(ArithmeticException|NumberFormatException e){throw invalid("JVM heap exceeds native accounting bounds.");}}
    private static boolean hash(String value){return value!=null&&value.matches("[a-f0-9]{64}");}
    private static boolean kafkaId(String value){return value!=null&&value.matches("[A-Za-z0-9_-]{22}");}
    private static boolean nonzeroKafkaId(String value){return kafkaId(value)&&!"AAAAAAAAAAAAAAAAAAAAAA".equals(value);}
    private static boolean unitName(String value){return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9_@.-]{0,180}\\.service")&&!value.contains("..");}
    private static boolean unitFile(String file,String unit){return canonicalPath(file)&&Path.of(file).getFileName().toString().equals(unit)&&List.of("/etc/systemd/system/","/usr/lib/systemd/system/","/lib/systemd/system/").stream().anyMatch(file::startsWith);}
    private static boolean binding(Participant p,String logical,String target){if(logical.equals(p.configPath()))return config(target)&&Path.of(logical).getFileName().equals(Path.of(target).getFileName());if(unitFile(logical,p.unit()))return unitFile(target,p.unit());return p.storage().stream().anyMatch(s->logical.equals(s.path()+"/meta.properties"))&&canonicalPath(target)&&target.endsWith("/meta.properties")&&path(target.substring(0,target.lastIndexOf('/')));}
    private static boolean canonicalPath(String value){return value!=null&&value.matches("/[A-Za-z0-9_./-]+")&&!value.endsWith("/")&&Path.of(value).normalize().toString().replace('\\','/').equals(value);}
    private static boolean path(String value){return canonicalPath(value)&&List.of("/opt/","/var/","/data/","/srv/","/app/").stream().anyMatch(value::startsWith);}
    public static boolean javaHomePath(String value){return path(value)||canonicalPath(value)&&value.matches("/usr/lib/jvm/[A-Za-z0-9_.-]+");}
    private static boolean config(String value){return canonicalPath(value)&&(path(value)||List.of("/etc/kafka/","/etc/tantor-kafka/","/etc/confluent/").stream().anyMatch(value::startsWith))&&Set.of("broker.properties","controller.properties","server.properties").contains(Path.of(value).getFileName().toString());}
    private static boolean overlap(String a,String b){return a.equals(b)||a.startsWith(b+"/")||b.startsWith(a+"/");}
    private static boolean port(int value){return value>0&&value<=65535;}
    private static boolean ip(String value){if(value==null)return false;try{if(value.contains(":")){if(!value.matches("[A-Fa-f0-9:]+"))return false;}else{String[] parts=value.split("\\.",-1);if(parts.length!=4)return false;for(String part:parts)if(!part.matches("0|[1-9][0-9]{0,2}")||Integer.parseInt(part)>255)return false;}java.net.InetAddress.getByName(value);return true;}catch(Exception e){return false;}}
    private static String canonicalIp(String value){try{return java.net.InetAddress.getByName(value).getHostAddress();}catch(Exception e){throw invalid("Literal IP required.");}}
    private static boolean sameIp(String a,String b){return ip(a)&&ip(b)&&canonicalIp(a).equals(canonicalIp(b));}
    private static String endpoint(String host,int port){return (host.contains(":")?"["+host+"]":host)+":"+port;}
    private static IllegalArgumentException invalid(String message){return new IllegalArgumentException(message);}
}
