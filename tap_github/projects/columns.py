# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from tap_github.streams import get_bookmark
from tap_github.gh_client import authed_get_all_pages


def get_all_project_columns(project_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_columns", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0
    with metrics.record_counter('project_columns') as counter:
        for response in authed_get_all_pages(
                'project_columns',
                'https://api.github.com/projects/{}/columns?sort=created_at&direction=desc'.format(project_id)
        ):
            project_columns = response.json()
            for r in project_columns:
                r['_sdc_repository'] = repo_path
                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state
                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()
                yield rec
    return state
