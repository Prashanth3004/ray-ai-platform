import ray
import ray.util

# Connect to the existing cluster and verify the custom worker resource.
ray.init(address="auto", include_dashboard=False)

print("LIVE NODE RESOURCES:")
for node in ray.nodes():
    if node.get("Alive"):
        print(node["NodeManagerAddress"], node.get("Resources", {}))

# Require the custom resource so Ray schedules this actor on the designated worker.
@ray.remote(resources={"entity_resolver": 1})
class EntityResolver:
    def location(self):
        return ray.util.get_node_ip_address()

actor = EntityResolver.remote()
print("ENTITY RESOLVER NODE IP:", ray.get(actor.location.remote()))
ray.kill(actor)
