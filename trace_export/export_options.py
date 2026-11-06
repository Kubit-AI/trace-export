"""Shared sampling, time bounds, query budget, and output behavior."""
import argparse
from datetime import datetime, timedelta, timezone
import shutil
import sys


def query_timeout(value):
    value = int(value)
    if not 1 <= value <= 172800:
        raise argparse.ArgumentTypeError('Must be between 1 and 172800 seconds')
    return value


def add_options(cli):
    modes = cli.add_mutually_exclusive_group()
    modes.add_argument('--sample', dest='sample', action='store_true', default=True,
                       help='Sample matching rows (default: 10 rows, last 24 hours)')
    modes.add_argument('--no-sample', dest='sample', action='store_false',
                       help='Export without a default row cap; requires time bounds or --all-time and --output')
    cli.add_argument('--all-time', action='store_true', help='Explicitly query all history; cannot combine with time bounds')
    cli.add_argument('--query-timeout', type=query_timeout, default=300, metavar='SECONDS',
                     help='Server-side statement timeout (default: 300 seconds)')
    cli.add_argument('--order-by-time', action='store_true', help='Sort results by timestamp (can increase query cost)')


def resolve_options(args, cli, now=None):
    now = now or datetime.now(timezone.utc)
    if args.all_time:
        if args.since or args.until:
            cli.error('--all-time cannot be combined with --since or --until')
    elif args.sample:
        args.until = args.until or now
        args.since = args.since or args.until - timedelta(hours=24)
    elif not args.since or not args.until:
        cli.error('--no-sample requires both --since and --until, or --all-time')
    if args.since and args.until and args.since >= args.until:
        cli.error('--since must be earlier than --until')
    if args.sample and args.limit is None:
        args.limit = 10
    if not args.sample and args.output is None:
        cli.error('--no-sample requires --output')
    if args.output is None and args.json_summary:
        cli.error('--json-summary requires --output; stdout cannot contain both trace data and a summary')
    if args.output is None and args.overwrite:
        cli.error('--overwrite requires --output')


def describe_export(args):
    window = 'all history' if args.all_time else f'[{args.since.isoformat()}, {args.until.isoformat()})'
    mode = 'sample' if args.sample else 'export'
    ordering = 'timestamp order' if args.order_by_time else 'unsorted'
    print(f'{mode.capitalize()}: {window}; limit={args.limit if args.limit is not None else "unlimited"} '
          f'database rows; query timeout={args.query_timeout}s; {ordering}.', file=sys.stderr)
    if args.sample:
        print('Sample is not random, representative, or necessarily the newest data.', file=sys.stderr)


def emit_output(args, path):
    if args.output is None:
        # Called only after the complete download/conversion succeeds.
        with path.open(encoding='utf-8') as handle:
            shutil.copyfileobj(handle, sys.stdout)
        sys.stdout.flush()
