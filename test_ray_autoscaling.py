import ray

# JM Comment: Connect to the existing cluster for a controlled autoscaling test.
ray.init(address="auto", include_dashboard=False)

@ray.remote(resources={"entity_resolver": 1})
class EntityResolver:
    def location(self):
        import ray.util
        return ray.util.get_node_ip_address()

# JM Comment: Three actors create demand for three worker custom-resource slots.
actors = [EntityResolver.remote() for _ in range(3)]
locations = ray.get([actor.location.remote() for actor in actors], timeout=180)

print("ACTOR NODE IPS:", locations)
print("DISTINCT NODES:", sorted(set(locations)))
print("LIVE RAY WORKERS:", [
    n["NodeManagerAddress"] for n in ray.nodes()
    if n.get("Alive") and "entity_resolver" in n.get("Resources", {})
])

input("Keep actors allocated while checking Kubernetes pods; press Enter to release them...")
for actor in actors:
    ray.kill(actor)
print("Actors released; scale-down can now be observed.")
ray.shutdown()
