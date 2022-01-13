# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from tap_github.streams import get_bookmark
from tap_github.gh_client import authed_get_all_pages


def get_all_events(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `created_at`
    # https://developer.github.com/v3/issues/events/#list-events-for-a-repository
    # 'https://api.github.com/repos/{}/issues/events?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0
    with metrics.record_counter('events') as counter:
        for response in authed_get_all_pages(
                'events',
                'https://api.github.com/repos/{}/events?sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for r in events:
                r['_sdc_repository'] = repo_path
                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = r.get('created_at') if r.get('updated_at') is None else r.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state
                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state
