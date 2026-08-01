"""Durable, content-free state transitions for isolated BL-21 experiments."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Mapping

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.knowledge.experiments import (
    Campaign,
    CampaignState,
    Candidate,
    CandidateState,
    EvaluationMetrics,
    ExperimentError,
    ExperimentPolicy,
    PromotionDecision,
    config_sha256,
    create_campaign,
    evaluation_metrics_record,
    normalize_money,
    phase_timing_summary,
    reject_content_fields,
    require_safe_identifier,
    require_sha256,
    resume_campaign,
    transition_campaign,
    transition_candidate,
)
from src.models.knowledge import ExperimentCampaign, ExperimentCampaignLock, ExperimentCandidate


class ExperimentRepository:
    """Small transactional store; it never accepts or persists retrieval content."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_or_get_campaign(
        self,
        *,
        campaign_key: str,
        channel_sha256: str,
        dataset_sha256: str,
        source_snapshot_sha256: str,
        source_snapshot_table_count: int,
        configuration: Mapping[str, object],
        policy: ExperimentPolicy,
    ) -> ExperimentCampaign:
        require_safe_identifier(campaign_key, "campaign_key")
        require_sha256(channel_sha256, "channel_sha256")
        require_sha256(source_snapshot_sha256, "source_snapshot_sha256")
        if not isinstance(source_snapshot_table_count, int) or isinstance(source_snapshot_table_count, bool) or source_snapshot_table_count < 1:
            raise ExperimentError("source_snapshot_table_count must be positive")
        reject_content_fields(configuration)
        campaign = create_campaign(campaign_key, configuration, dataset_sha256)
        policy_hash = config_sha256(policy)
        existing = (await self._session.execute(
            select(ExperimentCampaign).where(ExperimentCampaign.campaign_key == campaign_key)
        )).scalar_one_or_none()
        if existing is not None:
            expected = (
                campaign.config_sha256,
                campaign.dataset_sha256,
                source_snapshot_sha256,
                source_snapshot_table_count,
                policy_hash,
                campaign.resume_key,
                normalize_money(policy.budget_usd),
                channel_sha256,
            )
            actual = (
                existing.config_sha256,
                existing.dataset_sha256,
                existing.source_snapshot_sha256,
                existing.source_snapshot_table_count,
                existing.policy_sha256,
                existing.resume_key,
                normalize_money(existing.budget_usd),
                existing.channel_sha256,
            )
            if actual != expected:
                raise ExperimentError("campaign key already belongs to different immutable inputs")
            return existing
        record = ExperimentCampaign(
            campaign_key=campaign_key,
            channel_sha256=channel_sha256,
            dataset_sha256=campaign.dataset_sha256,
            source_snapshot_sha256=source_snapshot_sha256,
            source_snapshot_table_count=source_snapshot_table_count,
            config_sha256=campaign.config_sha256,
            policy_sha256=policy_hash,
            resume_key=campaign.resume_key,
            budget_usd=policy.budget_usd,
            status=campaign.state,
        )
        self._session.add(record)
        await self._session.flush()
        return record

    async def acquire_campaign_lock(self, campaign: ExperimentCampaign) -> ExperimentCampaignLock:
        """Serialize all runners for one hashed channel without storing its name."""
        dialect = self._session.bind.dialect.name if self._session.bind else ""
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert

            await self._session.execute(insert(ExperimentCampaignLock).values(
                channel_sha256=campaign.channel_sha256,
                campaign_id=campaign.id,
            ).on_conflict_do_nothing(index_elements=["channel_sha256"]))
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert

            await self._session.execute(insert(ExperimentCampaignLock).values(
                channel_sha256=campaign.channel_sha256,
                campaign_id=campaign.id,
            ).on_conflict_do_nothing(index_elements=["channel_sha256"]))
        lock = (await self._session.execute(
            select(ExperimentCampaignLock)
            .where(ExperimentCampaignLock.channel_sha256 == campaign.channel_sha256)
            .with_for_update()
        )).scalar_one_or_none()
        if lock is None:
            lock = ExperimentCampaignLock(channel_sha256=campaign.channel_sha256, campaign_id=campaign.id)
            self._session.add(lock)
            await self._session.flush()
            return lock
        if lock.campaign_id != campaign.id:
            raise ExperimentError("another campaign already owns this channel lock")
        return lock

    async def resume_campaign(self, campaign: ExperimentCampaign, configuration: Mapping[str, object]) -> ExperimentCampaign:
        reject_content_fields(configuration)
        resumed = resume_campaign(_campaign_value(campaign), configuration, campaign.dataset_sha256)
        campaign.status = resumed.state
        campaign.completed_at = None
        await self.acquire_campaign_lock(campaign)
        await self._session.flush()
        return campaign

    async def transition_campaign(self, campaign: ExperimentCampaign, target: CampaignState) -> ExperimentCampaign:
        if target == CampaignState.RUNNING:
            await self.acquire_campaign_lock(campaign)
        transitioned = transition_campaign(_campaign_value(campaign), target)
        campaign.status = transitioned.state
        now = datetime.now(timezone.utc)
        if target == CampaignState.RUNNING and campaign.started_at is None:
            campaign.started_at = now
        if target in {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED}:
            campaign.completed_at = now
            await self._session.execute(delete(ExperimentCampaignLock).where(
                ExperimentCampaignLock.channel_sha256 == campaign.channel_sha256,
                ExperimentCampaignLock.campaign_id == campaign.id,
            ))
        await self._session.flush()
        return campaign

    async def claim_candidate(
        self,
        campaign: ExperimentCampaign,
        *,
        hypothesis_id: str,
        configuration: Mapping[str, object],
        index_label: str,
        projected_cost_usd: Decimal,
    ) -> ExperimentCandidate | None:
        if campaign.status != CampaignState.RUNNING:
            raise ExperimentError("candidates may only be claimed by a running campaign")
        await self.acquire_campaign_lock(campaign)
        require_safe_identifier(hypothesis_id, "hypothesis_id")
        require_safe_identifier(index_label, "index_label")
        reject_content_fields(configuration)
        config_hash = config_sha256(configuration)
        projected = normalize_money(projected_cost_usd)
        if projected < 0:
            raise ExperimentError("projected_cost_usd must be non-negative")
        candidate = (await self._session.execute(
            select(ExperimentCandidate)
            .where(ExperimentCandidate.campaign_id == campaign.id, ExperimentCandidate.config_sha256 == config_hash)
            .with_for_update()
        )).scalar_one_or_none()
        if candidate is None:
            candidate = ExperimentCandidate(
                campaign_id=campaign.id,
                hypothesis_id=hypothesis_id,
                config_sha256=config_hash,
                index_label=index_label,
                projected_cost_usd=projected,
            )
            self._session.add(candidate)
            await self._session.flush()
        elif (candidate.hypothesis_id, candidate.index_label, normalize_money(candidate.projected_cost_usd or 0)) != (hypothesis_id, index_label, projected):
            raise ExperimentError("candidate config hash already belongs to different immutable inputs")
        if candidate.status in {CandidateState.EVALUATED, CandidateState.FAILED, CandidateState.SKIPPED}:
            return None
        candidate.status = transition_candidate(_candidate_value(candidate), CandidateState.RUNNING).state
        candidate.claimed_at = candidate.claimed_at or datetime.now(timezone.utc)
        await self._session.flush()
        return candidate

    async def complete_candidate(
        self,
        candidate: ExperimentCandidate,
        *,
        dev_metrics: EvaluationMetrics,
        holdout_metrics: EvaluationMetrics,
        phase_timings_ms: Mapping[str, list[float]],
        actual_cost_usd: Decimal,
        decision: PromotionDecision,
    ) -> ExperimentCandidate:
        if not isinstance(decision, PromotionDecision):
            raise ExperimentError("decision must be a PromotionDecision")
        actual = normalize_money(actual_cost_usd)
        projected = normalize_money(candidate.projected_cost_usd or 0)
        if actual < 0 or actual > projected:
            raise ExperimentError("actual_cost_usd must be within its projected reservation")
        candidate.status = transition_candidate(_candidate_value(candidate), CandidateState.EVALUATED).state
        candidate.dev_metrics = evaluation_metrics_record(dev_metrics)
        candidate.holdout_metrics = evaluation_metrics_record(holdout_metrics)
        candidate.phase_percentiles = phase_timing_summary(phase_timings_ms)
        candidate.actual_cost_usd = actual
        candidate.promotion_decision = decision
        candidate.completed_at = datetime.now(timezone.utc)
        await self._session.flush()
        return candidate

    async def fail_candidate(self, candidate: ExperimentCandidate, reason_code: str) -> ExperimentCandidate:
        require_safe_identifier(reason_code, "reason_code")
        candidate.status = transition_candidate(_candidate_value(candidate), CandidateState.FAILED).state
        candidate.failure_reason = reason_code
        candidate.completed_at = datetime.now(timezone.utc)
        await self._session.flush()
        return candidate


def _campaign_value(record: ExperimentCampaign) -> Campaign:
    return Campaign(record.campaign_key, record.config_sha256, record.dataset_sha256, record.resume_key, record.status)


def _candidate_value(record: ExperimentCandidate) -> Candidate:
    # Candidate transition rules do not depend on the parent key; avoid a lazy load.
    return Candidate(record.config_sha256, "0" * 64, record.status)
