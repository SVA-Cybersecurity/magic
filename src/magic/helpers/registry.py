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


class CrawlerRegistry:

    def __init__(self):
        self.crawlers = {}

    def register(self, name: str, crawler_cls: type):
        self.crawlers[name] = crawler_cls

    def get(self, name: str) -> type:
        crawler_class = self.crawlers.get(name)
        if not crawler_class:
            raise ValueError(f"No Crawler found for name: {name}")
        return crawler_class


def register_crawler(_cls: object = None, *, name: str):
    def decorator(cls):
        registration_name = name if name else cls.__name__

        CRAWLER_REGISTRY.register(registration_name, cls)

        return cls

    return decorator


class EnricherRegistry:

    def __init__(self):
        self.enrichers = {}

    def register(self, name: str, crawler_cls: type):
        self.enrichers[name] = crawler_cls

    def get(self, name: str) -> type:
        enricher_class = self.enrichers.get(name)
        if not enricher_class:
            raise ValueError(f"No Enricher found for name: {name}")
        return enricher_class


def register_enricher(_cls: object = None, *, name: str):
    def decorator(cls):
        registration_name = name if name else cls.__name__

        ENRICHER_REGISTRY.register(registration_name, cls)

        return cls

    return decorator


CRAWLER_REGISTRY = CrawlerRegistry()
ENRICHER_REGISTRY = EnricherRegistry()
