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
import os
import argparse
import asyncio
import sys
import time
import pkgutil
import importlib
import pathlib
import shutil
from .helpers.config import parse_config, Settings, INIT_CONFIG_FILES, INIT_DIRECTORIES
from .helpers.utils import check_output_dir, log_task, close_coroutines
from .helpers.logging import Logger
from .helpers.registry import CRAWLER_REGISTRY, ENRICHER_REGISTRY
from .helpers.permissions import PermissionValidator

APP_DIR = os.path.dirname(os.path.realpath(__file__))


def init():
    """create config files and necessary directories"""
    for dir in INIT_DIRECTORIES:
        path = pathlib.Path(dir)
        if not path.exists():
            path.mkdir(parents=True)
            print(f"Directory '{dir}/' created!")
        else:
            print(f"Directory '{dir}/' exists - skipping!")

    for source, target in INIT_CONFIG_FILES:
        target_path = pathlib.Path(target)
        if not target_path.exists():
            with importlib.resources.files("magic").joinpath(source) as src:
                shutil.copy(src, target_path)
                print(f"'{target}' created!")
        else:
            print(f"'{target}' exists - skipping!")


def load_modules(logger: Logger) -> None:
    from . import crawler, enricher

    for package in [crawler, enricher]:
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            try:
                importlib.import_module(f".{module_name}", package=package.__package__)
            except Exception as e:
                logger.error(f"Failed to load module - '{module_name}': {e}")


async def run(
    reports_dir: str,
    logger: Logger,
    settings: Settings,
    actions: list,
    enrichments: list,
    output_dir: str,
    debug: bool,
    manifest: bool,
) -> None:
    data_crawlers = []
    data_enricher = []

    if actions.root:
        for item in actions.root:
            if item.type in CRAWLER_REGISTRY.crawlers:
                handler_class = CRAWLER_REGISTRY.get(item.type)
                handler = handler_class(
                    reports_dir=reports_dir,
                    settings=settings,
                    output_dir=os.path.join(output_dir, item.type),
                    config=item,
                    debug=debug,
                )

                data_crawlers.append(handler)

    tasks = [
        log_task(task, crawler.logger) for crawler in data_crawlers if crawler is not None for task in crawler.get_tasks()
    ]

    required_permissions = set()

    for crawler in data_crawlers:
        required_permissions.update(crawler.get_collected_permissions())

    if manifest or settings.permission_preflight_check:
        permission_validator = PermissionValidator(settings, reports_dir, required_permissions, debug)

    if manifest:
        """just output permission manifest.json"""
        await permission_validator.create_manifest()
        close_coroutines(tasks)
        sys.exit(1)

    if settings.permission_preflight_check:

        """permission validation if enabled"""
        result = await asyncio.gather(*[asyncio.create_task(permission_validator.validate())])

        if not result[0]:
            logger.error("Permission validation failed - please set permissions accordingly!")
            close_coroutines(tasks)
            sys.exit(1)

    await asyncio.gather(*tasks)

    if enrichments:
        for key, value in enrichments:
            if key in ENRICHER_REGISTRY.enrichers:
                handler_class = ENRICHER_REGISTRY.get(key)
                handler = handler_class(
                    reports_dir=reports_dir, output_dir=output_dir, settings=settings, config=value, debug=debug
                )

                data_enricher.append(handler)

    """ run jsonl enrichments as prerequesite for other enrichments """
    jsonl_enricher = ENRICHER_REGISTRY.get("jsonl")(
        reports_dir=reports_dir, output_dir=output_dir, settings=settings, config={}, debug=debug
    )

    await asyncio.gather(*[log_task(task, jsonl_enricher.logger) for task in jsonl_enricher.get_tasks()])

    tasks = [
        log_task(task, enricher.logger) for enricher in data_enricher if enricher is not None for task in enricher.get_tasks()
    ]

    """ chained enrichment """
    for task in tasks:
        await task


def bootstrap_argparser():
    parser = argparse.ArgumentParser(
        add_help=True,
        description="MAGIC - Microsoft Azure Graph Informations Crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-c",
        "--config",
        action="store",
        help="Path to configuration file (default: config.yaml in working directory or module directory)",
        default=os.path.join(os.getcwd(), "config.yaml"),
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        action="store",
        help="Directory for results (default: output in working directory or module directory)",
        default=os.path.join(os.getcwd(), "output"),
    )
    parser.add_argument(
        "--reports-dir",
        action="store",
        help="Directory for logs (default: logs in working directory or module directory)",
        default=os.path.join(os.getcwd(), "logs"),
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging", default=False)
    parser.add_argument("--manifest", action="store_true", help="Generate permissions manifest", default=False)

    try:
        args = parser.parse_args()
        return args
    except argparse.ArgumentError:
        parser.print_help(sys.stderr)
        sys.exit(1)


def main() -> None:
    args = bootstrap_argparser()

    logger = Logger("magic", args.reports_dir, args.debug)
    check_output_dir(args.reports_dir, None)
    logger = logger.bootstrap()

    load_modules(logger)

    check_output_dir(args.output_dir, logger)

    settings, actions, enrichments = parse_config(args.config, logger)

    logger.info("Magic beginning to crawl")
    seconds = time.perf_counter()
    try:
        asyncio.run(run(args.reports_dir, logger, settings, actions, enrichments, args.output_dir, args.debug, args.manifest))
    except RuntimeError:
        sys.exit(1)
    elapsed = time.perf_counter() - seconds
    logger.info("Magic executed in {0:0.2f} seconds".format(elapsed))


if __name__ == "__main__":
    main()
