
# Verify remote task execution in the local Ray runtime.

import ray


@ray.remote
def process_record(value: int) -> int:
    return value * value


def main():
    ray.init(num_cpus=2, include_dashboard=False)

    try:
        values = list(range(1, 11))
        results = ray.get([
            process_record.remote(value)
            for value in values
        ])

        print("Input records:", len(values))
        print("Results:", results)
        print("Ray task execution: SUCCESS")

        assert results == [value * value for value in values]
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
