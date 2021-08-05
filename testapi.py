from apispec import APISpec
from pprint import pprint
from apispec.ext.marshmallow import MarshmallowPlugin
from marshmallow import Schema, fields
from apispec_webframeworks.flask import FlaskPlugin

spec = APISpec(
    title="Gisty",
    version="1.0.0",
    openapi_version="3.0.2",
    info=dict(description="A minimal gist API"),
    plugins=[MarshmallowPlugin()]
)

spec.components.schema(
    "Gist",
    {
        "properties": {
            "id": {"type": "integer", "format": "int64"},
            "name": {"type": "string"},
        }
    },
)
spec.path(
    path="/gist/{gist_id}",
    operations=dict(
        get=dict(
            responses={"200": {"content": {"application/json": {"schema": "Gist"}}}}
        )
    ),
)

pprint(spec.to_dict())
