#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -------------------------------------------------- #
# METADATA                                           #
# -------------------------------------------------- #
__author__ = "Alexander Goedeke"
__version__ = "0.5.0"


# -------------------------------------------------- #
# IMPORTS                                           #
# -------------------------------------------------- #
from typing import List, Optional
from datetime import datetime
from ..interfaces.crawler import BaseCrawler
from ..helpers.utils import TaskWrapper, write_json_to_file
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.config import RETENTION_TEAMS_CALL_RECORDS


@register_crawler(name="m365_teams_call_records")
class TeamsCallRecordsCrawler(BaseCrawler):

    task_prefix: str = "crawl"
    OUTPUT_FILE_SUFFIX = ".jsonl"
    RETENTION = RETENTION_TEAMS_CALL_RECORDS

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        tasks = []
        
        if self.config.external_user_principal_names:
            # Targeted mode: search for specific external users
            for external_upn in self.config.external_user_principal_names:
                task_name = f"crawl_teams_call_records_targeted_{external_upn}"
                tasks.append(
                    TaskWrapper(
                        name=task_name,
                        coroutine=self.crawl_teams_call_records_targeted(external_upn)
                    )
                )
        else:
            # Sweep mode: find all calls with any external participant
            tasks.append(
                TaskWrapper(
                    name="crawl_teams_call_records_sweep",
                    coroutine=self.crawl_teams_call_records_sweep()
                )
            )

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "CallRecords.Read.All")])
    async def crawl_teams_call_records_targeted(self, external_upn: str) -> None:
        """Search for calls with a specific external user (v2 variant with CallOutcome + Duration)."""
        
        date_start, date_end = self._read_date_fields()
        
        self.logger.info(
            f"Searching Teams call records for external user '{external_upn}' "
            f"from {date_start} to {date_end}"
        )

        output_filename = self.create_search_identifier(
            "teams_call_records",
            "targeted",
            external_upn
        )
        output_file_path = f"{self.output_dir}/{output_filename}{self.OUTPUT_FILE_SUFFIX}"

        await self.ensure_graph_client()
        
        call_records = await self._fetch_call_records(date_start, date_end)
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for call_record in call_records:
                call_id = call_record.id
                
                # Fetch participants
                participants = await self._fetch_participants(call_id)
                
                # Check if external user is in this call
                external_participant = self._find_participant_by_upn(participants, external_upn)
                
                if not external_participant:
                    continue
                
                # Fetch sessions for CallOutcome + Duration
                sessions = await self._fetch_sessions(call_id)
                
                # Extract internal participants
                internal_participants = self._get_internal_participants(
                    participants,
                    self.settings.auth.tenant_id
                )
                
                # Create output record for each internal participant
                for internal_participant in internal_participants:
                    call_outcome, duration_seconds = self._calculate_call_outcome_duration(
                        sessions,
                        internal_participant
                    )
                    
                    output_record = {
                        "callId": call_id,
                        "startDateTime": call_record.start_date_time.isoformat() if call_record.start_date_time else None,
                        "duration": call_record.duration.isoformat() if call_record.duration else None,
                        "internalParticipant": internal_participant,
                        "externalParticipant": external_participant,
                        "callOutcome": call_outcome,
                        "durationSeconds": duration_seconds,
                    }
                    
                    write_json_to_file(json_input=output_record, file=f)
        
        self.logger.info(f"Teams call records exported to {output_file_path}")

    @require_permissions([(ServicePrincipalType.GRAPH_API, "CallRecords.Read.All")])
    async def crawl_teams_call_records_sweep(self) -> None:
        """Sweep for all calls with any external participant (v2 variant with CallOutcome + Duration)."""
        
        date_start, date_end = self._read_date_fields()
        
        self.logger.info(
            f"Sweeping Teams call records for any external participants "
            f"from {date_start} to {date_end}"
        )

        output_filename = self.create_search_identifier("teams_call_records", "sweep", None)
        output_file_path = f"{self.output_dir}/{output_filename}{self.OUTPUT_FILE_SUFFIX}"

        await self.ensure_graph_client()
        
        call_records = await self._fetch_call_records(date_start, date_end)
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for call_record in call_records:
                call_id = call_record.id
                
                # Fetch participants
                participants = await self._fetch_participants(call_id)
                
                # Classify participants
                internal_participants = self._get_internal_participants(
                    participants,
                    self.settings.auth.tenant_id
                )
                external_participants = self._get_external_participants(
                    participants,
                    self.settings.auth.tenant_id
                )
                
                # Only include calls with both internal and external participants
                if not internal_participants or not external_participants:
                    continue
                
                # Fetch sessions for CallOutcome + Duration
                sessions = await self._fetch_sessions(call_id)
                
                # Create output record for each (internal, external) pair
                for internal_participant in internal_participants:
                    for external_participant in external_participants:
                        call_outcome, duration_seconds = self._calculate_call_outcome_duration(
                            sessions,
                            internal_participant
                        )
                        
                        output_record = {
                            "callId": call_id,
                            "startDateTime": call_record.start_date_time.isoformat() if call_record.start_date_time else None,
                            "duration": call_record.duration.isoformat() if call_record.duration else None,
                            "internalParticipant": internal_participant,
                            "externalParticipant": external_participant,
                            "callOutcome": call_outcome,
                            "durationSeconds": duration_seconds,
                        }
                        
                        write_json_to_file(json_input=output_record, file=f)
        
        self.logger.info(f"Teams call records exported to {output_file_path}")

    async def _fetch_call_records(self, date_start: datetime, date_end: datetime) -> List:
        """Fetch call records for date range using pagination."""
        try:
            start_iso = date_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = date_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            filter_query = f"startDateTime ge {start_iso} and startDateTime le {end_iso}"
            
            # Get the request builder and apply filter
            from msgraph_beta.generated.communications.call_records.call_records_request_builder import (
                CallRecordsRequestBuilder
            )
            
            builder = self.graph_client.communications.call_records
            query_params = CallRecordsRequestBuilder.CallRecordsRequestBuilderGetQueryParameters(
                filter=filter_query
            )
            req_config = CallRecordsRequestBuilder.CallRecordsRequestBuilderGetRequestConfiguration(
                query_parameters=query_params
            )
            
            # Use pagination-aware method, output_file_path=None returns list
            call_records = await self.make_graph_request_with_retry_and_pagination(
                output_file_path=None,
                request_func=builder,
                request_configuration=req_config
            )
            
            self.logger.debug(f"Fetched {len(call_records) if call_records else 0} call records")
            return call_records if call_records else []
            
        except Exception as e:
            self.logger.error(f"Failed to fetch call records: {e}")
            return []

    async def _fetch_participants(self, call_id: str) -> List:
        """Fetch participants for a call record using pagination."""
        try:
            builder = self.graph_client.communications.call_records.by_call_record_id(call_id).participants_v2
            
            # Use pagination-aware method, output_file_path=None returns list
            participants = await self.make_graph_request_with_retry_and_pagination(
                output_file_path=None,
                request_func=builder
            )
            
            self.logger.debug(f"Fetched {len(participants) if participants else 0} participants for call {call_id}")
            return participants if participants else []
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch participants for call {call_id}: {e}")
            return []

    async def _fetch_sessions(self, call_id: str) -> List:
        """Fetch sessions with segments for a call record using pagination."""
        try:
            builder = self.graph_client.communications.call_records.by_call_record_id(call_id).sessions
            
            # Use pagination-aware method, output_file_path=None returns list
            sessions = await self.make_graph_request_with_retry_and_pagination(
                output_file_path=None,
                request_func=builder
            )
            
            self.logger.debug(f"Fetched {len(sessions) if sessions else 0} sessions for call {call_id}")
            return sessions if sessions else []
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch sessions for call {call_id}: {e}")
            return []

    def _find_participant_by_upn(self, participants: List, upn: str) -> Optional[dict]:
        """Find a participant by user principal name (case-insensitive)."""
        upn_lower = upn.lower()
        
        for participant in participants:
            participant_upn = getattr(participant, "user_principal_name", None)
            if participant_upn and participant_upn.lower() == upn_lower:
                return self._participant_to_dict(participant)
        
        return None

    def _get_internal_participants(self, participants: List, tenant_id: str) -> List[dict]:
        """Filter participants belonging to the tenant (internal)."""
        internal = []
        
        for participant in participants:
            tenant_id_attr = getattr(participant, "tenant_id", None)
            if tenant_id_attr and tenant_id_attr == tenant_id:
                internal.append(self._participant_to_dict(participant))
        
        return internal

    def _get_external_participants(self, participants: List, tenant_id: str) -> List[dict]:
        """Filter participants NOT belonging to the tenant (external)."""
        external = []
        
        for participant in participants:
            tenant_id_attr = getattr(participant, "tenant_id", None)
            # External: has a different tenant ID, or no tenant ID (guest/phone)
            if not tenant_id_attr or tenant_id_attr != tenant_id:
                # Classify if it's truly external (not phone-only, etc.)
                endpoint_type = getattr(participant, "endpoint_type", None)
                if endpoint_type and endpoint_type.lower() not in ["phone", "application", "device", "encrypted"]:
                    external.append(self._participant_to_dict(participant))
        
        return external

    def _participant_to_dict(self, participant) -> dict:
        """Convert participant object to dictionary by extracting all attributes."""
        result = {}
        for attr in dir(participant):
            if not attr.startswith('_') and not callable(getattr(participant, attr, None)):
                result[attr] = getattr(participant, attr, None)
        return result

    def _calculate_call_outcome_duration(self, sessions: List, participant: dict) -> tuple:
        """Calculate CallOutcome and DurationSeconds for a participant from sessions/segments."""
        
        if not sessions:
            return "Unknown", 0
        
        participant_id = participant.get("id")
        if not participant_id:
            return "Unknown", 0
        
        total_duration = 0
        call_outcome = "NoSessions"
        
        for session in sessions:
            session_participant_id = getattr(session, "caller_id", None) or getattr(session, "participant_id", None)
            
            if session_participant_id == participant_id:
                # This session belongs to this participant
                segments = getattr(session, "segments", [])
                
                if not segments:
                    call_outcome = "NoAnswer"
                    continue
                
                # Sum up durations from segments
                for segment in segments:
                    if hasattr(segment, "duration"):
                        total_duration += segment.duration
                
                # Determine outcome from segments
                for segment in segments:
                    media_stream_type = getattr(segment, "media_stream", None)
                    call_state = getattr(segment, "call_state", None)
                    
                    if call_state and call_state.lower() == "connected":
                        call_outcome = "Connected"
                    elif call_state and call_state.lower() == "failed":
                        call_outcome = "Failed"
        
        return call_outcome, total_duration
