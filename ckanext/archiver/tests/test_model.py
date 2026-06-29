from __future__ import annotations
from typing import Any

import pytest

import ckanext.archiver.model as archiver_model

Archival = archiver_model.Archival


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestArchival:
    def test_create(self, resource: dict[str, Any]):
        archival = Archival.create(resource["id"])
        assert isinstance(archival, Archival)
        assert archival.package_id == resource["package_id"]
