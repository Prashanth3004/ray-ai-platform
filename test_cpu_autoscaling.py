import time
import ray

# Connect to the existing Ray cluster and create temporary CPU demand.
ray.init(address="auto", include_dashboard=False)

@ray.remote(num_cpus=1)
def autoscaling_probe():
    import time
    # Keep each task active long enough for the autoscaler to react.
    time.sleep(45)
    return ray.util.get_node_ip_address()

tasks = [autoscaling_probe.remote() for _ in range(6)]
print("Submitted 6 CPU tasks.")
print("Task execution nodes:", ray.get(tasks, timeout=240))
print("CPU autoscaling probe completed.")
ray.shutdown()
