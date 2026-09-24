"""Deterministic synthetic data for the explicit offline preview mode."""

import math
import pandas as pd


def preview_results(scope, include_messages=False):
    buckets = pd.date_range(scope.start, scope.end, freq=f'{scope.bucket_seconds}s', inclusive='left')
    parsed = [int(18000 + 4000 * math.sin(index / 5) + (index % 7) * 180) for index in range(len(buckets))]
    rejected = [160 if index % 13 == 0 else 4 + index % 9 for index in range(len(buckets))]
    volume = pd.DataFrame({'bucket': buckets, 'rows_parsed': parsed, 'errors': rejected})
    volume['rows_ingested'] = volume['rows_parsed'] - volume['errors']
    total_rows = int(volume['rows_ingested'].sum())
    total_errors = int(volume['errors'].sum())
    channel_names = [scope.channel] if scope.channel else ['demo-channel-01', 'demo-channel-02', 'demo-channel-03']
    row_parts = [total_rows // len(channel_names)] * len(channel_names)
    row_parts[-1] += total_rows - sum(row_parts)
    error_parts = [total_errors // len(channel_names)] * len(channel_names)
    error_parts[-1] += total_errors - sum(error_parts)
    lifecycle = pd.DataFrame({'bucket': [buckets[0], buckets[len(buckets) // 2]], 'opens': [len(channel_names), 1], 'drops': [0, 1]})
    errors = pd.DataFrame([{
        'event_time': buckets[-1], 'event_name': 'row_error',
        'channel_name': channel_names[0], 'error_type': None,
        'error_code': 'SYNTHETIC_VALIDATION_ERROR',
    }])
    if include_messages:
        errors['error_message'] = 'Synthetic example: a row did not match the demo schema.'
    return {
        'summary': pd.DataFrame([{
            'event_count': len(buckets) * 2 + int(lifecycle[['opens', 'drops']].sum().sum()) + 1,
            'commit_events': len(buckets), 'latency_events': len(buckets),
            'rows_ingested': total_rows, 'rows_parsed': sum(parsed), 'errors': total_errors,
            'measured_samples': max(len(buckets) - 2, 0),
            'avg_latency_ms': 4320, 'p95_latency_ms': 7850, 'latest_event': buckets[-1],
        }]),
        'volume': volume,
        'channels': pd.DataFrame({'channel_name': channel_names, 'rows_ingested': row_parts, 'errors': error_parts}),
        'lifecycle': lifecycle,
        'errors': errors,
    }
