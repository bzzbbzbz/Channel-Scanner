from src.knowledge.experiment_abstention import AbstentionSample, select_abstention_threshold, should_abstain


def test_threshold_is_selected_only_from_development_scores_and_prefers_stricter_tie() -> None:
    threshold = select_abstention_threshold([
        AbstentionSample(True, 0.9),
        AbstentionSample(True, 0.8),
        AbstentionSample(False, 0.2),
        AbstentionSample(False, 0.1),
    ])

    assert threshold == 0.8
    assert should_abstain(0.2, threshold)
    assert not should_abstain(0.9, threshold)
