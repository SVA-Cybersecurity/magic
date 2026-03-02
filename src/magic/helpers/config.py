#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -------------------------------------------------- #
# METADATA                                           #
# -------------------------------------------------- #
__author__ = "Alexander Goedeke"
__version__ = "0.5.0"


# -------------------------------------------------- #
# IMPORTS                                            #
# -------------------------------------------------- #
import yaml
import json
import sys
from datetime import datetime
from .logging import Logger
from pydantic import BaseModel, EmailStr, BeforeValidator, RootModel, Field, field_validator, model_validator
from typing import List, Optional, Any, Annotated, Union, Literal, Dict
from pydantic import ValidationError
from ..enricher.jsonl import Jsonl
from enum import Enum

# -------------------------------------------------- #
# CONSTANTS                                          #
# -------------------------------------------------- #
PREFIX_CRAWL = "crawl"
PREFIX_OUTPUT = "output"

INIT_CONFIG_FILES = [("config_example.yaml", "config.yaml"), ("available_crawls.yaml", "available_crawls.yaml")]

INIT_DIRECTORIES = ["output", "logs"]

RETENTION_DEFAULT = 30
RETENTION_SIGN_IN = 30
RETENTION_AUDIT = 30
RETENTION_MESSAGE_TRACES = 90
RETENTION_UAL = 180
RETENTION_MESSAGES = 3650  # 10 years


# -------------------------------------------------- #
# CUSTOM DATA TYPES                                  #
# -------------------------------------------------- #
def none_to_list(v: Any) -> List[str]:
    if v is None:
        return []
    return v


ListOfStrings = Annotated[List[str], BeforeValidator(none_to_list)]


class BaseAuditConfig(BaseModel):
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None

    @model_validator(mode='before')
    def check_date_range(cls, values):
        date_start = values.get("date_start")
        date_end = values.get("date_end")

        if date_end and date_start:
            """check valid date range"""
            if date_start > date_end:
                raise ValueError(f"date_start ({date_start}) has to be lower than date_end ({date_end})")

        return values


class M365UALConfig(BaseAuditConfig):
    type: Literal["m365_ual"] = "m365_ual"
    search_name_prefix: Optional[str] = "DataCrawler"
    keyword: Optional[str] = ""
    service: Optional[str] = ""
    user_principal_names: Optional[List[str]] = []
    record_types: ListOfStrings = []
    operations: ListOfStrings = []
    ip_addresses: ListOfStrings = []
    administrative_unit_id_filters: ListOfStrings = []
    object_id_filters: ListOfStrings = []
    number_interval_days: int = 7


class SignInType(str, Enum):
    INTERACTIVE_USER = ("interactiveUser", "(signInEventTypes/any(t: t eq 'interactiveUser'))")
    NON_INTERACTIVE_USER = ("nonInteractiveUser", "(signInEventTypes/any(t: t eq 'nonInteractiveUser'))")
    USER = (
        "user",
        "(signInEventTypes/any(t: t eq 'interactiveUser' or t eq 'nonInteractiveUser'))",
    )
    SERVICE_PRINCIPAL = ("servicePrincipal", "(signInEventTypes/any(t: t eq 'servicePrincipal'))")
    MANAGED_IDENTITY = ("managedIdentity", "(signInEventTypes/any(t: t eq 'managedIdentity'))")

    def __new__(cls, value: str, filter_query: str):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.filter_query = filter_query
        return obj

    @property
    def odata_filter(self) -> str:
        return self.filter_query


class M365SigninConfig(BaseAuditConfig):
    type: Literal["m365_signin"] = "m365_signin"
    user_principal_names: Optional[List[EmailStr]] = []
    sign_in_type: SignInType = SignInType.USER
    number_interval_days: int = 7


class M365AuditConfig(BaseAuditConfig):
    type: Literal["m365_audit"] = "m365_audit"
    user_principal_names: Optional[List[EmailStr]] = []
    number_interval_days: int = 7


class M365MessageTracesConfig(BaseAuditConfig):
    type: Literal["m365_message_traces"] = "m365_message_traces"
    from_ip: Optional[str] = None
    to_ip: Optional[str] = None
    subject: Optional[str] = None
    subject_filter_type: Optional[str] = None
    recipient_address: Optional[str] = None
    sender_address: Optional[str] = None
    number_interval_days: int = 7

    @field_validator('subject_filter_type')
    def validate_filter_type(cls, v, values):
        if values.data.get('subject') and v not in ['Contains', 'EndsWith', 'StartsWith']:
            raise ValueError("subject_filter_type must be one of 'Contains', 'EndsWith', 'StartsWith'")
        return v


class M365MessageTracesPWSHConfig(BaseAuditConfig):
    type: Literal["m365_message_traces_pwsh"] = "m365_message_traces_pwsh"
    from_ip: Optional[str] = None
    to_ip: Optional[str] = None
    subject: Optional[str] = None
    subject_filter_type: Optional[str] = None
    recipient_addresses: Optional[List[EmailStr]] = []
    sender_addresses: Optional[List[EmailStr]] = []
    result_size: Optional[int] = None
    number_interval_days: int = 10

    @field_validator('number_interval_days')
    def check_number_interval_days(cls, v, values):
        if v < 1 or v > 10:
            raise ValueError("number_interval_days must be between 1 and 10")
        return v

    @model_validator(mode='before')
    def check_if_sender_or_recipient(cls, values):
        if values.get('recipient_addresses') is None and values.get('sender_addresses') is None:
            raise ValueError("Either sender_addresses or recipient_addresses must be set.")
        return values

    @field_validator('subject_filter_type')
    def validate_filter_type(cls, v, values):
        if values.data.get('subject') and v not in ['Contains', 'EndsWith', 'StartsWith']:
            raise ValueError("subject_filter_type must be one of 'Contains', 'EndsWith', 'StartsWith'")
        return v


class M365MessagesConfig(BaseAuditConfig):
    type: Literal["m365_messages"] = "m365_messages"
    user_principal_names: Optional[List[EmailStr]] = None

    @model_validator(mode='before')
    @classmethod
    def check_user_principal_names(cls, values):
        if not values.get("user_principal_names"):
            raise ValueError("user_principal_names are required!")
        return values


class M365MessageConfig(BaseAuditConfig):
    type: Literal["m365_message"] = "m365_message"
    user_principal_name: Optional[EmailStr] = None
    message_id: Optional[str] = None
    internet_message_id: Optional[str] = None

    @model_validator(mode='before')
    def check_user_principal_name(cls, values):
        if not values.get("user_principal_name"):
            raise ValueError("user_principal_name is required!")

        if not values.get("message_id") and not values.get("internet_message_id"):
            raise ValueError("either an message_id or internet_message_id is required!")

        return values


class M365Config(BaseModel):
    type: Literal["m365"] = "m365"
    message_rules: bool
    authentication_methods: bool
    mailbox_settings: bool
    users_transitive_member_of: bool
    service_principals_transitive_member_of: bool
    directory_provisioning: bool
    directory_roles: bool
    risk_detections: bool
    risky_users: bool
    applications: bool
    conditional_access: bool
    security: bool
    service_principals: bool
    users: bool
    groups: bool
    devices: bool
    permission_grants: bool
    recommendations: bool
    attack_simulation: bool

    _non_method_attributes: frozenset = frozenset({"type"})

    @property
    def _methods(self) -> dict:
        return self.model_dump(exclude=self._non_method_attributes)

    def __iter__(self):
        yield from self._methods

    def __contains__(self, item):
        return item in self._methods

    def items(self):
        return self._methods.items()


CrawlItem = Annotated[
    Union[
        M365UALConfig,
        M365SigninConfig,
        M365AuditConfig,
        M365MessageTracesConfig,
        M365MessageTracesPWSHConfig,
        M365MessagesConfig,
        M365MessageConfig,
        M365Config,
    ],
    Field(discriminator='type'),
]


class CrawlConfig(RootModel):
    root: List[CrawlItem]


class TimesketchConfig(BaseModel):
    enabled: bool
    output_filename: str
    input_filename: str = Jsonl.OUTPUT_FILENAME


class HashConfig(BaseModel):
    enabled: bool
    output_filename: str
    output_filename_csv: str


class IpAPIConfig(BaseModel):
    enabled: bool
    output_filename: str
    input_filename: str = Jsonl.OUTPUT_FILENAME


class S3UploadConfig(BaseModel):
    enabled: bool
    bucket_path: str
    input_filename: str = "timesketch.jsonl"


class EnrichConfig(BaseModel):
    timesketch: TimesketchConfig
    ipapi: IpAPIConfig
    hash: HashConfig
    s3_upload: Optional[S3UploadConfig] = None


class AuthSettings(BaseModel):
    client_secret: Optional[str]
    client_id: Optional[str]
    tenant_id: Optional[str]


class IpAPISettings(BaseModel):
    endpoint: Optional[str] = None
    key: Optional[str] = None
    cert: str | bool = False


class S3Settings(BaseModel):
    endpoint_url: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    verify_ssl: bool = True


class Defaults(BaseModel):
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    user_principal_names: Optional[List[EmailStr]] = None


class Settings(BaseModel):
    permission_preflight_check: bool
    auth: AuthSettings
    ipapi: Optional[IpAPISettings] = None
    s3: Optional[S3Settings] = None
    defaults: Defaults


def set_defaults(actions: List[Optional[Dict]], defaults: Defaults, logger: Logger):
    logger.debug("Start setting defaults")
    for action in actions:
        logger.debug(f"Crawl block before: {action}")
        """ attr has to be existent on action item """
        for key, val in defaults:
            try:
                if not action[key]:
                    logger.debug(f"{action.get('type')} - change value for key {key} from {action[key]} to {val}")
                    action[key] = val
                else:
                    logger.debug(f"{action.get('type')} - value for key {key} was already set to {action[key]}")
            except Exception:
                pass

        logger.debug(f"Crawl block after: {action}")

    return actions


def parse_config(config_file: str, logger: Logger) -> dict:
    try:
        with open(config_file, "r") as f:
            raw_config = yaml.safe_load(f)

        logger.debug(json.dumps(raw_config, indent=2, default=str))

        enrich_config = EnrichConfig(**raw_config["enrich"])
        settings = Settings(**raw_config["settings"])

        crawl_config = CrawlConfig.model_validate(set_defaults(raw_config["crawl"], settings.defaults, logger))

        return settings, crawl_config, enrich_config

    except ValidationError as e:
        for error in e.errors():
            logger.error(
                f"Error occured while parsing config file: {e.title} - {'.'.join(map(str, error.get('loc')))} - {error.get('msg')}"
            )
        sys.exit(1)
