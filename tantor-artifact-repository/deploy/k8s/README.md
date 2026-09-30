# Artifact repository Kubernetes release manifest

`artifact-repository.yaml.template` is intentionally not directly deployable
because its image reference is not digest locked. Generate a release manifest
with an approved registry image reference:

```powershell
./prepare-release.ps1 `
  -ImageReference 'registry.translab.io/tantor/artifact-repository:1.0.0@sha256:<64-hex-digest>'
```

The generated `artifact-repository.release.yaml` is ignored by Git and belongs
in the signed release bundle. Review its digest, security context, resource
limits, probes and secret references before applying it.
