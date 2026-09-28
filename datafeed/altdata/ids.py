from __future__ import annotations

from dataclasses import dataclass

from datafeed.errors import DataFeedError


@dataclass(frozen=True)
class AltDataId:
    provider: str
    series: str
    output_id: str


def parse_alt_data_id(raw_id: str) -> AltDataId:
    """Parse provider.series by splitting on the first dot."""
    if "." not in raw_id:
        raise DataFeedError(f"Alt data id '{raw_id}' must be in 'provider.series' form")

    provider, series = raw_id.split(".", 1)
    provider = provider.strip().upper()
    series = series.strip().upper()

    if not provider or not series:
        raise DataFeedError(f"Alt data id '{raw_id}' must be in 'provider.series' form")

    return AltDataId(
        provider=provider,
        series=series,
        output_id=f"{provider}.{series}",
    )


def normalize_alt_data_ids(raw_ids: list[str]) -> list[AltDataId]:
    """Normalize IDs and deduplicate after normalization."""
    normalized: list[AltDataId] = []
    seen: set[str] = set()

    for raw_id in raw_ids:
        parsed = parse_alt_data_id(raw_id)
        if parsed.output_id in seen:
            continue
        seen.add(parsed.output_id)
        normalized.append(parsed)

    return normalized
