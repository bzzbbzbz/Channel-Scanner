"""Private Qdrant BM25/vector experiment primitives."""

from src.knowledge.experiment_hybrid import HybridMethod, HybridPost, PrivateHybridIndex, bm25_tokens


def test_bm25_tokenization_keeps_versions_dates_and_abbreviations() -> None:
    assert bm25_tokens("Qwen-3.6, MCP и 2026-08-11") == ["qwen-3.6", "mcp", "и", "2026-08-11"]


def test_private_hybrid_index_compares_all_declared_fusion_methods(tmp_path) -> None:
    index = PrivateHybridIndex(tmp_path / "private", dimensions=2, identity={"candidate": "test"})
    try:
        index.build([
            HybridPost(10, (1.0, 0.0), "Qwen-3.6 MCP protocol 2026"),
            HybridPost(20, (0.0, 1.0), "Kubernetes deployment guide"),
        ])
        for method in HybridMethod:
            result = index.query((1.0, 0.0), "Qwen-3.6 MCP", method=method)
            assert result.method == method
            assert result.post_ids[0] == 10
    finally:
        index.close()


def test_private_hybrid_index_reuses_an_existing_local_collection(tmp_path) -> None:
    root = tmp_path / "private"
    posts = [
        HybridPost(10, (1.0, 0.0), "Qwen MCP protocol"),
        HybridPost(20, (0.0, 1.0), "Kubernetes deployment"),
    ]
    first = PrivateHybridIndex(root, dimensions=2, identity={"candidate": "test"})
    try:
        first.build(posts)
    finally:
        first.close()
    second = PrivateHybridIndex(root, dimensions=2, identity={"candidate": "test"})
    try:
        second.build(posts)
        assert second.query((1.0, 0.0), "Qwen MCP", method=HybridMethod.DENSE).post_ids[0] == 10
    finally:
        second.close()
