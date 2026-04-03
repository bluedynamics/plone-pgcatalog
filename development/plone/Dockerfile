FROM plone/plone-backend:6.2.0a1

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Released packages from PyPI (install into /app venv, not system Python)
RUN uv pip install --python /app/bin/python --no-cache \
    "zodb-pgjsonb>=1.3" \
    "zodb-json-codec>=1.6"

# Local: plone-pgcatalog
COPY sources/plone-pgcatalog /tmp/plone-pgcatalog
RUN cd /tmp/plone-pgcatalog && \
    SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0 uv pip install --python /app/bin/python --no-cache . && \
    rm -rf /tmp/plone-pgcatalog

# Override zope.conf with pgjsonb storage config
COPY sources/plone-pgcatalog/development/plone/zope.conf /app/etc/zope.conf

EXPOSE 8080
