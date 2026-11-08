"""Run the required experiments 1-7 (add --bonus1 / --bonus2 for the bonus experiments)."""
import subprocess, sys, os
here = os.path.dirname(os.path.abspath(__file__))
scripts = ["exp1_shard_distribution.py", "exp2_load_distribution.py", "exp3_concurrent_replicas.py",
           "exp4_replica_failure.py", "exp5_local_and_cross_transfers.py", "exp6_coordinator_recovery.py",
           "exp7_concurrent_cross_shard.py"]
if "--bonus1" in sys.argv: scripts.append("exp8_bonus_live_resharding.py")
if "--bonus2" in sys.argv: scripts.append("exp9_bonus_self_healing.py")
failed = []
for s in scripts:
    if subprocess.run([sys.executable, os.path.join(here, s)]).returncode:
        failed.append(s)
print("\n" + "#" * 70)
print("ALL EXPERIMENTS PASSED" if not failed else "FAILED: " + ", ".join(failed))
sys.exit(1 if failed else 0)
