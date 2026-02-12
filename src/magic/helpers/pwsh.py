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
import re
from dataclasses import dataclass
from warnings import warn
from kiota_abstractions.serialization import Parsable, ParsableFactory, ParseNode, SerializationWriter
from kiota_abstractions.request_information import RequestInformation
from kiota_abstractions.request_adapter import RequestAdapter
from kiota_abstractions.base_request_configuration import RequestConfiguration
from kiota_abstractions.default_query_parameters import QueryParameters
from kiota_abstractions.base_request_builder import BaseRequestBuilder
from kiota_abstractions.method import Method
from typing import Optional, Any, Union, Self
from msgraph_beta.generated.models.entity import Entity


class CmdletParameters(Parsable):
    def __init__(self, allowed_fields: list[str] = None, **kwargs):
        self._fields = set(allowed_fields or kwargs.keys())

        # Initialize all allowed fields to None
        for field in self._fields:
            setattr(self, field, None)

        # Set provided values
        for key, value in kwargs.items():
            if key in self._fields:
                setattr(self, key, value)

    def get_field_deserializers(self):
        return {key: lambda n, k=key: setattr(self, k, n.get_str_value()) for key in self._fields}

    def serialize(self, writer: SerializationWriter):
        for key in self._fields:
            value = getattr(self, key, None)
            writer.write_str_value(key, value)

    @staticmethod
    def create_from_discriminator_value(parse_node: ParseNode):
        if parse_node is None:
            raise TypeError("parse_node cannot be null.")
        return CmdletParameters()


class CmdletInput(Parsable):
    def __init__(self, cmdlet_name: str = None, parameters: CmdletParameters = None):
        self.cmdlet_name = cmdlet_name
        self.parameters = parameters

    cmdlet_name: Optional[str] = None
    parameters: Optional[CmdletParameters] = None

    @staticmethod
    def create_from_discriminator_value(parse_node: ParseNode):
        if parse_node is None:
            raise TypeError("parse_node cannot be null.")
        return CmdletParameters()

    def get_field_deserializers(self):
        return {
            "CmdletName": lambda n: setattr(self, 'cmdlet_name', n.get_str_value()),
            "Parameters": lambda n: setattr(self, 'parameters', n.get_object_value(CmdletParameters)),
        }

    def serialize(self, writer: SerializationWriter):
        writer.write_str_value("CmdletName", self.cmdlet_name)
        writer.write_object_value("Parameters", self.parameters)


class CmdletRootModel(Parsable):

    cmdlet_input: Optional[CmdletInput] = None

    def __init__(self, cmdlet_input: CmdletInput = None):
        self.cmdlet_input = cmdlet_input

    @staticmethod
    def create_from_discriminator_value(parse_node: ParseNode):
        if parse_node is None:
            raise TypeError("parse_node cannot be null.")
        return CmdletParameters()

    def get_field_deserializers(self):
        return {
            "CmdletInput": lambda n: setattr(self, 'cmdlet_input', n.get_object_value(CmdletInput)),
        }

    def serialize(self, writer: SerializationWriter):
        writer.write_object_value("CmdletInput", self.cmdlet_input)

    def parse_pwsh_cmdlet_string(self, cmdlet_string: str) -> Self:
        # Extract the command part after the static message
        match = re.search(r'Get-(\w+)\s+(.*)', cmdlet_string)
        if not match:
            raise ValueError("Cmdlet not found in string")

        cmdlet_name = f"Get-{match.group(1)}"
        param_string = match.group(2)

        # Regex to extract parameters and values
        param_pattern = re.findall(r'-(\w+)\s+"([^"]+)"|-(\w+)\s+(\S+)', param_string)

        raw_params = {}
        for group in param_pattern:
            if group[0]:  # matched with quotes
                raw_params[group[0]] = group[1]
            else:  # matched without quotes
                raw_params[group[2]] = group[3]

        return CmdletRootModel(cmdlet_input=CmdletInput(cmdlet_name=cmdlet_name, parameters=CmdletParameters(**raw_params)))


class PowerShellModuleRequestBuilder(BaseRequestBuilder):
    """
    Provides operations to call a powershell property using the microsoft graph api.
    """

    def __init__(self, request_adapter: RequestAdapter, path_parameters: Union[str, dict[str, Any]]) -> None:
        """
        Instantiates a new PowerShellModuleRequestBuilder and sets the default values.
        param path_parameters: The raw url or the url-template parameters for the request.
        param request_adapter: The request adapter to use to execute the requests.
        Returns: None
        """
        super().__init__(
            request_adapter, "https://outlook.office365.com/adminapi/beta/{tenant_id}/InvokeCommand", path_parameters
        )

    async def post(
        self, body: CmdletRootModel, request_configuration: Optional[RequestConfiguration[QueryParameters]] = None
    ) -> Optional[Entity]:
        if body is None:
            raise TypeError("body cannot be null.")
        request_info = self.to_post_request_information(body, request_configuration)

        from msgraph_beta.generated.models.o_data_errors.o_data_error import ODataError

        error_mapping: dict[str, type[ParsableFactory]] = {
            "XXX": ODataError,
        }
        if not self.request_adapter:
            raise Exception("Http core is null")

        return await self.request_adapter.send_async(request_info, Entity, error_mapping)

    def to_post_request_information(
        self, body: CmdletRootModel, request_configuration: Optional[RequestConfiguration[QueryParameters]] = None
    ) -> RequestInformation:
        """
        Publish an app to the Microsoft Teams app catalog.Specifically, this API publishes the app to your organization's catalog (the tenant app catalog);the created resource has a distributionMethod property value of organization. The requiresReview property allows any user to submit an app for review by an administrator. Admins can approve or reject these apps via this API or the Microsoft Teams admin center.
        param body: The request body
        param request_configuration: Configuration for the request such as headers, query parameters, and middleware options.
        Returns: RequestInformation
        """
        if body is None:
            raise TypeError("body cannot be null.")
        request_info = RequestInformation(Method.POST, self.url_template, self.path_parameters)
        request_info.configure(request_configuration)
        request_info.headers.try_add("Accept", "application/json")
        request_info.headers.try_add("X-CmdletName", body.cmdlet_input.cmdlet_name)
        request_info.headers.try_add("X-ResponseFormat", "json")
        request_info.headers.try_add("X-ClientApplication", "ExoManagementModule")
        request_info.headers.try_add("Content-Type", "application/json")
        request_info.set_content_from_parsable(self.request_adapter, "application/json", body)
        return request_info

    def with_url(self, raw_url: str) -> Self:
        """
        Returns a request builder with the provided arbitrary URL. Using this method means any other path or query parameters are ignored.
        param raw_url: The raw URL to use for the request builder.
        Returns: PowerShellModuleRequestBuilder
        """
        if raw_url is None:
            raise TypeError("raw_url cannot be null.")
        return PowerShellModuleRequestBuilder(self.request_adapter, raw_url)

    @dataclass
    class PowerShellModuleRequestBuilderPostRequestConfiguration(RequestConfiguration[QueryParameters]):
        """
        Configuration for the request such as headers, query parameters, and middleware options.
        """

        warn(
            "This class is deprecated. Please use the generic RequestConfiguration class generated by the generator.",
            DeprecationWarning,
        )
