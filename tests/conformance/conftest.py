"""Conformance-test fixtures.

The `proxy_client` fixture (anvil + REST proxy) is defined at the top-level
`tests/conftest.py`. This file adds the OpenAPI schema loader and validator
factory used to assert real responses match the spec.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

_OPENAPI_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "superpowers"
    / "specs"
    / "2026-05-28-execution-rest-api-openapi.yaml"
)


@pytest.fixture(scope="session")
def openapi_spec() -> dict[str, Any]:
    with _OPENAPI_PATH.open() as f:
        spec: dict[str, Any] = yaml.safe_load(f)
    return spec


_BASE_URI = "urn:openapi"


@pytest.fixture(scope="session")
def schema_registry(openapi_spec) -> Registry:
    """A jsonschema Registry that resolves `#/components/schemas/Foo` refs."""
    resource = Resource(contents=openapi_spec, specification=DRAFT202012)
    return Registry().with_resource(_BASE_URI, resource)


@pytest.fixture(scope="session")
def make_validator(schema_registry):
    """Returns `make(ref_str) -> Draft202012Validator`.

    `ref_str` is the fragment portion of the OpenAPI document (e.g.
    `#/components/schemas/Problem`).
    """

    def _make(ref: str) -> Draft202012Validator:
        return Draft202012Validator(
            {"$ref": f"{_BASE_URI}{ref}"}, registry=schema_registry
        )

    return _make
