package io.translab.tantor.server.service;

import java.time.Instant;
import java.util.*;

/** Ordered executable checkpoint contract shared by controller and two-process operations. */
public final class KafkaExpansionWorkflow {
    private KafkaExpansionWorkflow() {}
    public enum Mode { STATIC, DYNAMIC }
    public enum Layout { CONTROLLER, SEPARATE }
    public enum Kind {
        FINAL_VALIDATE(false, "CANDIDATE_READY", "PARTICIPANTS_READY", "BASELINE_UNCHANGED"),
        PREPARE(true, "OWNED_CONFIG", "OWNED_UNIT", "EMPTY_STORAGE"),
        STAGE_VOTERS(true, "BACKUP_VERIFIED", "STAGED_CONFIG", "EXPECTED_VOTERS"),
        PRE_MAINTENANCE(false, "HEALTHY_QUORUM", "ZERO_URP", "STAGING_COMPLETE"),
        STOP(true, "UNIT_STOPPED", "REVIEWED_CONFIG"),
        APPLY_VOTERS(true, "UNIT_STOPPED", "BACKUP_VERIFIED", "EXPECTED_VOTERS"),
        VERIFY_CONFIG(false, "EXPECTED_VOTERS", "ALL_CONFIGS_CONSISTENT"),
        FORMAT(true, "OWNED_CONFIG", "MATCHING_STORAGE"),
        START(true, "OWNED_CONFIG", "MATCHING_STORAGE", "UNIT_ACTIVE"),
        CATCH_UP(false, "OBSERVER_DIRECTORY_MATCH", "BOUNDED_LAG", "BASELINE_UNCHANGED"),
        ADMIT(true, "ADMISSION_DISPATCHED"),
        VERIFY_CONTROLLER(false, "VOTER_IDENTITY_MATCH", "HEALTHY_QUORUM", "EXPECTED_VOTERS"),
        VERIFY_BROKER(false, "BROKER_IDENTITY_MATCH", "BROKER_UNFENCED"),
        VERIFY_HEALTH(false, "EXPECTED_MEMBERSHIP", "HEALTHY_QUORUM", "ZERO_URP", "SMOKE_VERIFIED"),
        PUBLISH(true, "ALL_PLANNED_ROLES_VERIFIED", "TASKS_DRAINED", "PUBLICATION_COMMITTED");
        private final boolean mutation;
        private final Set<String> required;
        Kind(boolean mutation, String... required) { this.mutation = mutation; this.required = Set.of(required); }
        public boolean mutation() { return mutation; }
        public Set<String> requiredFacts() { return required; }
    }
    public enum State { PENDING, INTENT, CONFIRMED }
    public record Participant(String hostId, String unit, int nodeId, String role) {
        public Participant {
            if (hostId == null || hostId.isBlank() || unit == null || !unit.matches("[A-Za-z0-9][A-Za-z0-9_@.-]{0,180}\\.service") || nodeId < 0
                    || !Set.of("broker", "controller", "broker,controller").contains(role)) throw new IllegalArgumentException("Incomplete participant unit identity.");
        }
        public String key() { return hostId + ":" + unit; }
        public boolean broker() { return role.contains("broker"); }
        public boolean controller() { return role.contains("controller"); }
    }
    public record Step(String id, Kind kind, String target, String role) {}
    public record Evidence(String planDigest, String stepId, String target, Instant observedAt, Set<String> facts) {}

    public static List<Step> compile(Mode mode, Layout layout, List<Participant> reviewedParticipants) {
        Objects.requireNonNull(mode); Objects.requireNonNull(layout);
        var participants = normalized(reviewedParticipants);
        if (participants.stream().noneMatch(Participant::controller) || participants.stream().noneMatch(Participant::broker))
            throw new IllegalArgumentException("Reviewed broker and controller participants are required.");
        var out = new ArrayList<Step>();
        add(out, Kind.FINAL_VALIDATE, "cluster", "all");
        add(out, Kind.PREPARE, "candidate:controller", "controller");
        if (layout == Layout.SEPARATE) add(out, Kind.PREPARE, "candidate:broker", "broker");
        if (mode == Mode.STATIC) {
            participants.forEach(p -> add(out, Kind.STAGE_VOTERS, p.key(), p.role()));
            add(out, Kind.PRE_MAINTENANCE, "cluster", "all");
            // Combined units stop in the broker pass and are never stopped a second time.
            participants.stream().filter(Participant::broker).forEach(p -> add(out, Kind.STOP, p.key(), p.role()));
            participants.stream().filter(p -> p.controller() && !p.broker()).forEach(p -> add(out, Kind.STOP, p.key(), p.role()));
            participants.forEach(p -> add(out, Kind.APPLY_VOTERS, p.key(), p.role()));
            add(out, Kind.VERIFY_CONFIG, "cluster", "all");
        }
        add(out, Kind.FORMAT, "candidate:controller", "controller");
        if (layout == Layout.SEPARATE) add(out, Kind.FORMAT, "candidate:broker", "broker");
        if (mode == Mode.STATIC) participants.stream().filter(Participant::controller).forEach(p -> add(out, Kind.START, p.key(), p.role()));
        add(out, Kind.START, "candidate:controller", "controller");
        if (mode == Mode.DYNAMIC) {
            add(out, Kind.CATCH_UP, "candidate:controller", "controller");
            add(out, Kind.ADMIT, "candidate:controller", "controller");
        }
        add(out, Kind.VERIFY_CONTROLLER, "candidate:controller", "controller");
        if (mode == Mode.STATIC) participants.stream().filter(p -> p.broker() && !p.controller()).forEach(p -> add(out, Kind.START, p.key(), p.role()));
        if (layout == Layout.SEPARATE) {
            add(out, Kind.START, "candidate:broker", "broker");
            add(out, Kind.VERIFY_BROKER, "candidate:broker", "broker");
        }
        add(out, Kind.VERIFY_HEALTH, "cluster", "all");
        add(out, Kind.PUBLISH, "cluster", "all");
        return List.copyOf(out);
    }
    private static List<Participant> normalized(List<Participant> input) {
        var byUnit = new TreeMap<String, Participant>();
        for (var p : input) {
            var old = byUnit.get(p.key());
            if (old == null) byUnit.put(p.key(), p);
            else {
                if (old.nodeId() != p.nodeId()) throw new IllegalArgumentException("One unit cannot own two Kafka node IDs.");
                String roles = old.role().equals(p.role()) ? p.role() : "broker,controller";
                byUnit.put(p.key(), new Participant(p.hostId(), p.unit(), p.nodeId(), roles));
            }
        }
        return List.copyOf(byUnit.values());
    }
    private static void add(List<Step> steps, Kind kind, String target, String role) {
        steps.add(new Step("%03d:%s:%s".formatted(steps.size(), kind, target), kind, target, role));
    }

    /** Any uncertain mutation remains INTENT and retains its parent reservation. */
    public static final class Checkpoints {
        private final String digest;
        private final List<Step> steps;
        private final Map<String, State> states;
        public Checkpoints(String digest, List<Step> steps, Map<String, State> durableStates) {
            if (digest == null || !digest.matches("[a-f0-9]{64}") || steps.isEmpty()) throw new IllegalArgumentException("Frozen plan digest and steps are required.");
            this.digest = digest; this.steps = List.copyOf(steps); this.states = new LinkedHashMap<>();
            var known = new HashSet<String>(); boolean incomplete = false;
            for (var step : steps) {
                if (!known.add(step.id())) throw new IllegalArgumentException("Duplicate expansion step.");
                State state = durableStates.getOrDefault(step.id(), State.PENDING);
                if (state == null || incomplete && state != State.PENDING) throw new IllegalArgumentException("Nonsequential durable expansion checkpoints.");
                if (state != State.CONFIRMED) incomplete = true;
                states.put(step.id(), state);
            }
            if (!known.containsAll(durableStates.keySet())) throw new IllegalArgumentException("Foreign expansion checkpoint.");
        }
        public Optional<Step> next() { return steps.stream().filter(s -> states.get(s.id()) != State.CONFIRMED).findFirst(); }
        public boolean reservationRequired() { return next().isPresent(); }
        public boolean canCancelBeforeMutation() { return steps.stream().noneMatch(s -> s.kind().mutation() && states.get(s.id()) != State.PENDING); }
        public boolean reconciliationRequired() { return next().map(s -> states.get(s.id()) == State.INTENT && s.kind().mutation()).orElse(false); }
        public void intent(String id) {
            var step = requireNext(id);
            if (states.get(id) == State.INTENT && step.kind().mutation()) throw new IllegalStateException("Reconcile the prior mutation before dispatching another task.");
            states.put(id, State.INTENT);
        }
        public void confirm(Evidence proof, Instant now) {
            var step = requireNext(proof.stepId());
            if (states.get(step.id()) != State.INTENT || !digest.equals(proof.planDigest()) || !step.target().equals(proof.target())
                    || proof.observedAt() == null || proof.observedAt().isBefore(now.minusSeconds(60)) || proof.observedAt().isAfter(now.plusSeconds(5))
                    || proof.facts() == null || !proof.facts().containsAll(step.kind().requiredFacts())) throw new IllegalStateException("Fresh stage-specific expansion evidence is required.");
            states.put(step.id(), State.CONFIRMED);
        }
        public Map<String, State> snapshot() { return Collections.unmodifiableMap(new LinkedHashMap<>(states)); }
        private Step requireNext(String id) { return next().filter(s -> s.id().equals(id)).orElseThrow(() -> new IllegalStateException("Expansion steps cannot be skipped, replayed or executed out of order.")); }
    }
}
