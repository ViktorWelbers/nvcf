# NVCF 1.1.1 candidate qualification

This candidate is a patch of NVCF 1.1.0. It selects these released stacks:

| Stack | Version | Source commit |
| --- | --- | --- |
| Self-Managed | 1.1.9 | 859ad9e45d45f0503ea3759d7b2d07f5a5eb75df |
| Compute Plane | 1.1.1 | daf87d512a7980e3124a4d69d5e449458a5bb5e9 |
| Observability | 1.1.0 | d1918a811df7e7bc6be7d6cc625844b6f5e30b42 |

The manifest records the original inventory checksums and the reviewed source
commit `5496884c4819b262ee2645c89f4751c356984a7f`. It remains in development
status until the release owner qualifies the complete combination.

## Required evidence

- Successful Assemble Umbrella Candidate run for the committed manifest.
  Record the candidate SHA, run URL, artifact name, manifest digest and archive
  digest before testing. Package validation does not establish runtime health.
- Inventory comparison against NVCF 1.0.4 and NVCF 1.1.0, with no backward
  artifact moves on either supported upgrade path.
- Relevant live installation and upgrade validation from both baselines using
  the exact assembled bundle and recorded deployment configuration.
- Customer availability and immutable identities for the selected images and
  charts, including runtime CA trust checks for affected images.
- OpenBao plugin, migration, secret injection and rollback validation for this
  combination, together with the applicable NATS, cert-manager and Cassandra
  upgrade checks.

Qualification is pending. Do not publish this candidate as a qualified release
or reuse QA results from a different stack selection without verifying that
the tested artifact bytes match.

Track qualification in [the release issue](https://github.com/dsx-ai-factory/nvcf/issues/3428).
Use the [umbrella release procedure](../../../docs/dev/umbrella-releases.md)
to assemble and validate the bundle. Preserve NVCF 1.1.0's manifest, tag and
published assets.
