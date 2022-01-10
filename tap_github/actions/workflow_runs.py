# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from gh_client import get_bookmark, authed_get_all_pages, authed_get


def get_all_workflow_runs(schemas, repo_path, state, mdata, start_date):
    '''
    https://docs.github.com/en/rest/reference/actions#list-workflow-runs-for-a-repository
    '''
    bookmark_value = get_bookmark(state, repo_path, "workflow_runs", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0
    with metrics.record_counter('workflow_runs') as counter:
        for response in authed_get_all_pages(
                'runs',
                'https://api.github.com/repos/{}/pulls?state=all&sort=updated&direction=desc'.format(repo_path)
        ):
            workflow_runs = response.json()
            extraction_time = singer.utils.now()
            for run in workflow_runs:
                print(run)
