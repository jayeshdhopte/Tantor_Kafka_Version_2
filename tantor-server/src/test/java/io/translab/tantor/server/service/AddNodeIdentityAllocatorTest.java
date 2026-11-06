package io.translab.tantor.server.service;
import io.translab.tantor.server.domain.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AddNodeIdentityAllocatorTest {
    Cluster cluster(int... brokers){var c=new Cluster();var services=new ArrayList<ClusterServiceAssignment>();var ids=new ArrayList<Integer>();for(int id:brokers){var s=new ClusterServiceAssignment();s.setRole("broker");s.setNodeId(id);services.add(s);ids.add(id);}var ctrl=new ClusterServiceAssignment();ctrl.setRole("controller");ctrl.setNodeId(101);services.add(ctrl);ids.add(101);c.setServices(services);c.setNodeIds(ids);return c;}
    @Test void usesBrokerSequenceInsteadOfControllerMaximum(){var c=cluster(1,2);var a=AddNodeIdentityAllocator.broker(c,Set.of(1,2,101));assertTrue(a.available());assertEquals(3,a.nodeId());}
    @Test void neverRenumbersOrFillsGapsInExistingPilot(){var c=cluster(1,102);assertEquals(103,AddNodeIdentityAllocator.broker(c,Set.of(1,101,102)).nodeId());assertEquals(List.of(1,102,101),c.getNodeIds());}
    @Test void collisionsAcrossAllRolesBlockInsteadOfSkipping(){var c=cluster(1,2);var a=AddNodeIdentityAllocator.broker(c,Set.of(1,2,3,101));assertFalse(a.available());assertEquals(3,a.nodeId());}
    @Test void overflowCannotAllocateANegativeIdentity(){assertFalse(AddNodeIdentityAllocator.broker(cluster(Integer.MAX_VALUE),Set.of()).available());}
}
