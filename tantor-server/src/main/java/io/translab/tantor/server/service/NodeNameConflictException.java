package io.translab.tantor.server.service;

public class NodeNameConflictException extends RuntimeException {
    public NodeNameConflictException(String nodeName) {
        super("Node-name '" + nodeName + "' is already registered to a different host-id.");
    }
}
