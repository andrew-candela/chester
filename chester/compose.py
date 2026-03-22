"""
A convenience script to aid spinning up the temporal resources.
Might help someone not familiar with Docker.
I normally don't like incorporating dev only scripts in the library like this.
"""

import subprocess


def docker_compose_up():
    subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "compose/.env",
            "-f",
            "compose/docker-compose-postgres.yml",
            "up",
        ],
        check=True,
    )


def docker_compose_down():
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "compose/docker-compose-postgres.yml",
            "down",
        ],
        check=True,
    )
