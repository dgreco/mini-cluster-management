# cluster/ — whole-cluster lifecycle

All playbooks accept `-e target=<node>` (any inventory pattern) to act on
some nodes only. For `shutdown.yml` and `power-on.yml` that changes what
happens: the whole cluster is quiesced and recovered with the subsystems'
`pre-shutdown.yml` / `post-startup.yml`, while some nodes of a running cluster
are taken out of and back into service one at a time with the same
`node-enter` / `node-exit` hooks as a rolling reboot (`tasks/node-enter.yml`,
`tasks/node-exit.yml`), so the rest of the cluster keeps serving.

| Playbook | What it does |
|---|---|
| `status.yml`    | `tpi power status` on the BMC, then ping + uptime of every node |
| `power-on.yml`  | `tpi power on` (all or `-n <slot>`), wait for SSH, then every subsystem's `post-startup.yml`; some nodes of a running cluster: `tasks/node-exit.yml` per node instead |
| `shutdown.yml`  | every subsystem's `pre-shutdown.yml`, `shutdown -h now`, wait for SSH to close, `tpi power off`; some nodes: `tasks/node-enter.yml` per node (`serial: 1`, the enter hooks undone on failure) instead of the pre-shutdown hooks |
| `power-off.yml` | hard `tpi power off`, no hooks. Only for nodes that are already unreachable |
| `reboot.yml`    | rolling reboot of the target nodes through `rolling-reboot.yml` |
| `rolling-reboot.yml` | the rolling reboot shared by `reboot.yml` and `update.yml`: pre-flight (`hooks/rolling-start.yml` of each subsystem: refuse unless healthy, k3s etcd snapshot, order requests), order (`reboot_order`), then per node with `serial: 1` and `any_errors_fatal`: `tasks/node-enter.yml` (the subsystems' `hooks/node-enter.yml`), reboot, `tasks/node-exit.yml` (the `hooks/node-exit.yml`, which wait for full health), then `hooks/rolling-end.yml` |
| `update.yml`    | `apt full-upgrade`, then `apt autoremove`, on every node in parallel (it first lists what will be upgraded, kept back and removed); with `-e reboot=true`, the same rolling reboot as `reboot.yml` for only the nodes that report `/var/run/reboot-required` |
| `baseline.yml`  | checks the hostname matches the inventory, base packages, safe apt upgrade, chrony, cgroup v2 in the RK1 u-boot environment (`--tags cgroup` for just that), reports pending reboots |

The BMC (`[bmc]` group) is BusyBox without Python, so it is driven with the
`raw` module only. Slot numbers come from the `turingpi_slot` host variable in
the inventory; verify them once with `tpi power status`.
