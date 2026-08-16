# Source provenance

The starter is based on the ExaWorks PSI/J Python implementation, release
`0.9.0`, Git commit `952f7ef29a960028b51e514dbed608c2f8c1ad1c`, released on
2023-03-17.

Upstream repository: <https://github.com/ExaWorks/psij-python>

The upstream source is distributed under the MIT License. The unmodified
license text is retained in `LICENSE`. The exercise reproduces defects that
were subsequently corrected in public upstream history. It contains no
cluster credentials, private hostnames, customer data, or proprietary source.

The runtime is Python 3.10.14 on Debian 11. Runtime wheels are stored in the
Docker build context and pinned by version and SHA-256 in `requirements.lock`.
No online installation is needed to build or verify the exercise after the
base image is available.
