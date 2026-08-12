"""Content-free selection of a fixed no-answer confidence threshold."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

from src.knowledge.experiments import ExperimentError


@dataclass(frozen=True, slots=True)
class AbstentionSample:
    expects_answer: bool
    confidence: float


def select_abstention_threshold(samples: Iterable[AbstentionSample]) -> float:
    """Maximise answer/no-answer accuracy on development; prefer stricter ties."""
    values = tuple(samples)
    if not values or not any(sample.expects_answer for sample in values) or not any(not sample.expects_answer for sample in values):
        raise ExperimentError("threshold selection needs both answerable and no-answer development cases")
    if any(not math.isfinite(sample.confidence) for sample in values):
        raise ExperimentError("abstention confidence must be finite")
    thresholds = sorted({sample.confidence for sample in values}, reverse=True)
    best_threshold = thresholds[0]
    best_correct = -1
    for threshold in thresholds:
        correct = sum((sample.confidence >= threshold) == sample.expects_answer for sample in values)
        if correct > best_correct or (correct == best_correct and threshold > best_threshold):
            best_threshold, best_correct = threshold, correct
    return best_threshold


def should_abstain(confidence: float, threshold: float) -> bool:
    if not math.isfinite(confidence) or not math.isfinite(threshold):
        raise ExperimentError("abstention confidence must be finite")
    return confidence < threshold
