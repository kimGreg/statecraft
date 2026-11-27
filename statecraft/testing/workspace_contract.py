"""The workspace files conformance suite (public testing API): the baseline every `Files`
implementation keeps. A provider runs it with a `files` fixture (a `Files` it opened):

    from statecraft.testing.workspace_contract import *  # noqa: F403

    @pytest.fixture
    async def files(tmp_path):
        return await MyWorkspace(...).open("w")

What must hold: `put` overwrites and `get` returns the last bytes; `get` of a missing file is
`FileNotFoundError`; `create` is create-only (False when the path exists, the content kept);
`exists` tells; `list(prefix)` lists the files under a folder (`a` does not list `ab/x`), sorted
or not, with full paths; paths are relative and clean, `..` is refused (`ValueError`), a leading
`/` is ignored; concurrent `create`s of one path have exactly one winner.
"""

from __future__ import annotations

import asyncio

import pytest


async def test_put_overwrites_and_get_reads_back(files) -> None:
    await files.put("shared/a.txt", b"one")
    await files.put("shared/a.txt", b"two")
    assert await files.get("shared/a.txt") == b"two"


async def test_a_missing_file_is_not_found(files) -> None:
    with pytest.raises(FileNotFoundError):
        await files.get("shared/none.txt")
    assert await files.exists("shared/none.txt") is False


async def test_create_is_create_only(files) -> None:
    assert await files.create("home/a/objects/x.json", b"first") is True
    assert await files.create("home/a/objects/x.json", b"second") is False
    assert await files.get("home/a/objects/x.json") == b"first"


async def test_concurrent_creates_have_one_winner(files) -> None:
    won = await asyncio.gather(*(files.create("shared/once.txt", b"%d" % i) for i in range(8)))
    assert won.count(True) == 1
    assert await files.get("shared/once.txt") == b"%d" % won.index(True)


async def test_list_is_by_folder(files) -> None:
    await files.put("shared/a/x.txt", b"1")
    await files.put("shared/ab/y.txt", b"2")
    await files.put("shared/a/sub/z.txt", b"3")
    assert sorted(await files.list("shared/a")) == ["shared/a/sub/z.txt", "shared/a/x.txt"]
    assert sorted(await files.list("")) == [
        "shared/a/sub/z.txt",
        "shared/a/x.txt",
        "shared/ab/y.txt",
    ]


async def test_paths_stay_inside(files) -> None:
    for bad in ("../x", "shared/../../x", "..", "."):
        with pytest.raises(ValueError):
            await files.put(bad, b"x")
        with pytest.raises(ValueError):
            await files.list(bad if bad not in (".",) else "../")
    await files.put("/shared/lead.txt", b"x")
    assert await files.get("shared/lead.txt") == b"x"


__all__ = [
    "test_a_missing_file_is_not_found",
    "test_concurrent_creates_have_one_winner",
    "test_create_is_create_only",
    "test_list_is_by_folder",
    "test_paths_stay_inside",
    "test_put_overwrites_and_get_reads_back",
]
