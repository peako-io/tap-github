# Python imports
# Third-Party imports
import singer
from singer import metadata
import singer.metrics as metrics
# Project imports
from tap_github.gh_client import authed_get_all_pages


def get_all_collaborators(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    with metrics.record_counter('collaborators') as counter:
        for response in authed_get_all_pages(
                'collaborators',
                'https://api.github.com/repos/{}/collaborators'.format(repo_path)
        ):
            collaborators = response.json()
            extraction_time = singer.utils.now()
            for collaborator in collaborators:
                collaborator['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(collaborator, schema, metadata=metadata.to_map(mdata))
                singer.write_record('collaborators', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'collaborator', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state
