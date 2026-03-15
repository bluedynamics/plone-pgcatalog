FROM plone/plone-backend:6.2.0a1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN uv pip install --python /app/bin/python --no-cache \
    "zodb-pgjsonb>=1.3" \
    "zodb-json-codec>=1.6" \
    "plone.pgcatalog"

COPY zope.conf /app/etc/zope.conf

EXPOSE 8080
