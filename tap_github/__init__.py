import argparse
import os
import json
import singer
from tap_github.gh_client import *
from tap_github.exceptions import *
from tap_github.settings import *
from tap_github.streams import *
from tap_github.teams import get_all_teams
from tap_github.issues.issues import get_all_issues, get_all_issue_events
from tap_github.issues.milestones import get_all_issue_milestones
from tap_github.issues.labels import get_all_issue_labels
from tap_github.issues.comments import get_all_comments
from tap_github.events import get_all_events
from tap_github.commits.commits import get_all_commits
from tap_github.commits.comments import get_all_commit_comments
from tap_github.projects.projects import get_all_projects
from tap_github.projects.cards import get_all_project_cards
from tap_github.projects.columns import get_all_project_columns
from tap_github.releases import get_all_releases
from tap_github.pull_requests.pulls import get_all_pull_requests
from tap_github.pull_requests.assignees import get_all_assignees
from tap_github.collaborators import get_all_collaborators
from tap_github.starrings import get_all_stargazers
from tap_github.actions.workflow_runs import get_all_workflow_runs

logger = singer.get_logger()


SYNC_FUNCTIONS = {
    'commits': get_all_commits,
    'comments': get_all_comments,
    'issues': get_all_issues,
    'assignees': get_all_assignees,
    'collaborators': get_all_collaborators,
    'pull_requests': get_all_pull_requests,
    'releases': get_all_releases,
    'stargazers': get_all_stargazers,
    'events': get_all_events,
    'issue_events': get_all_issue_events,
    'issue_milestones': get_all_issue_milestones,
    'issue_labels': get_all_issue_labels,
    'projects': get_all_projects,
    'commit_comments': get_all_commit_comments,
    'teams': get_all_teams,
    'workflow_runs': get_all_workflow_runs,
}

SUB_STREAMS = {
    'pull_requests': ['reviews', 'review_comments', 'pr_commits'],
    'projects': ['project_cards', 'project_columns'],
    'teams': ['team_members', 'team_memberships']
}


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract '{0}' data, "
                "to receive '{0}' data, you also need to select '{1}'.")

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def do_discover(config):
    verify_access_for_repo(config)
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    start_date = config['start_date'] if 'start_date' in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    repositories = list(filter(None, config['repository'].split(' ')))

    state = translate_state(state, catalog, repositories)
    singer.write_state(state)

    #pylint: disable=too-many-nested-blocks
    for repo in repositories:
        logger.info("Starting sync of repository: %s", repo)
        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema, stream['key_properties'])

                # get sync function and any sub streams
                sync_func = SYNC_FUNCTIONS[stream_id]
                sub_stream_ids = SUB_STREAMS.get(stream_id, None)

                # sync stream
                if not sub_stream_ids:
                    state = sync_func(stream_schema, repo, state, mdata, start_date)

                # handle streams with sub streams
                else:
                    stream_schemas = {stream_id: stream_schema}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                            stream_schemas[sub_stream_id] = sub_stream['schema']
                            singer.write_schema(sub_stream_id, sub_stream['schema'],
                                                sub_stream['key_properties'])

                    # sync stream and it's sub streams
                    state = sync_func(stream_schemas, repo, state, mdata, start_date)

                singer.write_state(state)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
