FROM python@sha256:8e53874607bf1b7e97ad9fab4ee1bc3b237731e45b481e037dd2a30f603b0ac7

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace/src

COPY vendor/ /opt/wheels/
COPY task/requirements.lock /tmp/requirements.lock
RUN python -m pip install \
        --disable-pip-version-check \
        --no-cache-dir \
        --no-index \
        --find-links=/opt/wheels \
        --require-hashes \
        -r /tmp/requirements.lock \
    && rm -rf /opt/wheels /tmp/requirements.lock \
    && useradd --create-home --uid 1000 --shell /bin/bash agent

COPY --chown=agent:agent task/ /workspace/

USER agent
WORKDIR /workspace
