# Python imports
import json
import sys
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from gh_client import authed_get_all_pages, authed_get
from streams import get_bookmark


logger = singer.get_logger()


def get_all_workflow_runs(schemas, repo_path, state, mdata, start_date):
    '''
    https://docs.github.com/en/rest/reference/actions#list-workflow-runs-for-a-repository
    '''
    bookmark_value = get_workflow_run_bookmark(state, repo_path, start_date)
    bookmark_time = get_workflow_run_bookmark_time(bookmark_value)
    with metrics.record_counter('workflow_runs') as counter:
        for response in authed_get_all_pages(
                'runs',
                'https://api.github.com/repos/{}/actions/runs'.format(repo_path)
        ):
            workflow_runs = response.json()
            extraction_time = singer.utils.now()

            for run in workflow_runs["workflow_runs"]:
                if bookmark_time and singer.utils.strptime_to_utc(run.get('updated_at')) < bookmark_time:
                    return state
                run['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    record = transformer.transform(run, schemas, metadata=metadata.to_map(mdata))
                    singer.write_record('workflow_runs', record, time_extracted=extraction_time)
                    singer.write_bookmark(
                        state,
                        repo_path,
                        'workflow_runs',
                        {'since': singer.utils.strftime(extraction_time)}
                    )
                    sys.stdout.flush()
                    counter.increment()
    return state


def get_workflow_run_bookmark(state, repo_path, start_date):
    bookmark_value = None
    if state:
        bookmark_value = get_bookmark(state, repo_path, "workflow_runs", "since", start_date)
    return bookmark_value


def get_workflow_run_bookmark_time(bookmark_value):
    bookmark_time = 0
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    return bookmark_time
