# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from tap_github.streams import get_bookmark
from tap_github.gh_client import authed_get_all_pages


def get_all_commit_comments(schemas, repo_path, state, mdata, start_date):
    # https://developer.github.com/v3/repos/comments/
    # updated_at? incremental
    # 'https://api.github.com/repos/{}/comments?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "commit_comments", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0
    with metrics.record_counter('commit_comments') as counter:
        for response in authed_get_all_pages(
                'commit_comments',
                'https://api.github.com/repos/{}/comments?sort=created_at&direction=desc'.format(repo_path)
        ):
            commit_comments = response.json()
            extraction_time = singer.utils.now()
            for r in commit_comments:
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
                singer.write_record('commit_comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'commit_comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state
