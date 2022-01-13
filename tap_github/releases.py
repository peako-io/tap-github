# Python imports
# Third-Party imports
import singer
from singer import metadata
import singer.metrics as metrics
# Project imports
from tap_github.gh_client import authed_get_all_pages


def get_all_releases(schemas, repo_path, state, mdata, _start_date):
    # Releases doesn't seem to have an `updated_at` property, yet can be edited.
    # For this reason and since the volume of release can safely be considered low,
    #    bookmarks were ignored for releases.
    with metrics.record_counter('releases') as counter:
        for response in authed_get_all_pages(
                'releases',
                'https://api.github.com/repos/{}/releases?sort=created_at&direction=desc'.format(repo_path)
        ):
            releases = response.json()
            extraction_time = singer.utils.now()
            for r in releases:
                r['_sdc_repository'] = repo_path
                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('releases', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'releases', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state
