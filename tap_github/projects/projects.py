# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from tap_github.streams import get_bookmark
from tap_github.gh_client import authed_get_all_pages
from exceptions import GithubException


logger = singer.get_logger()


def get_all_projects(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "projects", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0
    with metrics.record_counter('projects') as counter:
        try:
            #pylint: disable=too-many-nested-blocks
            for response in authed_get_all_pages(
                    'projects',
                    'https://api.github.com/repos/{}/projects?sort=created_at&direction=desc'.format(repo_path),
                    {
                        'Accept': 'application/vnd.github.inertia-preview+json' ,
                    }
            ):
                projects = response.json()
                extraction_time = singer.utils.now()
                for r in projects:
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
                    singer.write_record('projects', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'projects', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()
                    project_id = r.get('id')
                    # sync project_columns if that schema is present (only there if selected)
                    if schemas.get('project_columns'):
                        for project_column_rec in get_all_project_columns(project_id, schemas['project_columns'], repo_path, state, mdata, start_date):
                            singer.write_record('project_columns', project_column_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'project_columns', {'since': singer.utils.strftime(extraction_time)})

                            # sync project_cards if that schema is present (only there if selected)
                            if schemas.get('project_cards'):
                                column_id = project_column_rec['id']
                                for project_card_rec in get_all_project_cards(column_id, schemas['project_cards'], repo_path, state, mdata, start_date):
                                    singer.write_record('project_cards', project_card_rec, time_extracted=extraction_time)
                                    singer.write_bookmark(state, repo_path, 'project_cards', {'since': singer.utils.strftime(extraction_time)})
        except GithubException:
            logger.info("Projects are disabled on this repo %s", repo)
    return state
