# RisingWave Patches

This directory contains minimal patches to enable RisingWave integration with Nexora 2.

## Patch List

- `001-enable-external-election.patch` - Enable external election plugin (Phase 4)
- `002-expose-election-trait.patch` - Expose ElectionClient trait (Phase 4)

## Applying Patches

```bash
# Apply all patches
../scripts/apply-patches.sh

# Apply single patch manually
git apply patches/001-enable-external-election.patch
```

## Creating New Patches

```bash
# Make changes in vendor/risingwave/
cd vendor/risingwave
# ... edit files ...

# Create patch
git diff > ../../patches/003-my-change.patch
```

## Patch Guidelines

- Keep patches minimal (<100 lines each)
- One logical change per patch
- Document why the patch is needed
- Test that patches apply cleanly after RisingWave upgrades
