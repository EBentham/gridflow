"""The slow NESO tests over a real data root skip when ``GRIDFLOW_REAL_DATA_DIR`` is unset.

They used to default to ``C:/gridflow-data``: on any other machine that path is
wrong, and on the owner's it reads the live store without being asked to.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import test_neso_bal2ah_real_bodies
import test_neso_overlap_memory
import test_neso_reader_memory

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.mark.parametrize(
    "locate",
    [
        test_neso_bal2ah_real_bodies._root,
        test_neso_overlap_memory._root,
        test_neso_reader_memory._body,
    ],
    ids=["bal2ah", "overlap", "reader"],
)
def test_real_data_tests_skip_without_variable(
    locate: Callable[[], object], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each module's own path lookup skips before it touches any path."""
    monkeypatch.delenv("GRIDFLOW_REAL_DATA_DIR", raising=False)

    with pytest.raises(pytest.skip.Exception, match="GRIDFLOW_REAL_DATA_DIR"):
        locate()
