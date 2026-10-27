import ray
# Connect to the existing Ray cluster and inspect live node resources.
ray.init(address="auto")
print("CLUSTER RESOURCES:", ray.cluster_resources())
print("LIVE NODES:", [(n.get("NodeManagerAddress"), n.get("Resources", {})) for n in ray.nodes() if n.get("Alive")])
