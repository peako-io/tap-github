import os
import sys
import json
import collections
import time
import requests
import backoff
import singer

from singer import (bookmarks, metrics, metadata)
from simplejson import JSONDecodeError

session = requests.Session()
logger = singer.get_logger()

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

REQUIRED_CONFIG_KEYS = ['start_date', 'access_token', 'repository']

KEY_PROPERTIES = {
    'commits': ['sha'],
    'comments': ['id'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests':['id'],
    'stargazers': ['user_id'],
    'releases': ['id'],
    'reviews': ['id'],
    'review_comments': ['id'],
    'pr_commits': ['id'],
    'events': ['id'],
    'issue_events': ['id'],
    'issue_labels': ['id'],
    'issue_milestones': ['id'],
    'commit_comments': ['id'],
    'projects': ['id'],
    'project_columns': ['id'],
    'project_cards': ['id'],
    'repos': ['id'],
    'teams': ['id'],
    'team_members': ['id', 'team_slug'],
    'team_memberships': ['url'],
    'workflow_runs': ['id']
}

DEFAULT_SLEEP_SECONDS = 600
MAX_SLEEP_SECONDS = DEFAULT_SLEEP_SECONDS

class GithubException(Exception):
    pass

class BadCredentialsException(GithubException):
    pass

class AuthException(GithubException):
    pass

class NotFoundException(GithubException):
    pass

class BadRequestException(GithubException):
    pass

class InternalServerError(GithubException):
    pass

class UnprocessableError(GithubException):
    pass

class NotModifiedError(GithubException):
    pass

class MovedPermanentlyError(GithubException):
    pass

class ConflictError(GithubException):
    pass

class RateLimitExceeded(GithubException):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found. Alternatively the access_token is not valid for the resource"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}

def translate_state(state, catalog, repositories):
    '''
    This tap used to only support a single repository, in which case the
    state took the shape of:
    {
      "bookmarks": {
        "commits": {
          "since": "2018-11-14T13:21:20.700360Z"
        }
      }
    }
    The tap now supports multiple repos, so this function should be called
    at the beginning of each run to ensure the state is translate to the
    new format:
    {
      "bookmarks": {
        "singer-io/tap-adwords": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
        "singer-io/tap-salesforce": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    '''
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        for repo in repositories:
            if bookmarks.get_bookmark(state, repo, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, 'since'):
                new_state['bookmarks'][repo][stream_name]['since'] = bookmarks.get_bookmark(state, stream_name, 'since')

    return new_state


def get_bookmark(state, repo, stream_name, bookmark_key, start_date):
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)
    if start_date:
        return start_date
    return None

def raise_for_error(resp, source):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except JSONDecodeError:
        response_json = {}

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, Error: {}. Please refer \'{}\' for more details.".format(details, response_json.get("documentation_url"))
        logger.info(message)
        # don't raise a NotFoundException
        return None

    message = "HTTP-error-code: {}, Error: {}".format(
        error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error") if response_json == {} else response_json)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GithubException)
    raise exc(message) from None

def calculate_seconds(epoch):
    current = time.time()
    return int(round((epoch - current), 0))

def rate_throttling(response):
    if int(response.headers['X-RateLimit-Remaining']) == 0:
        seconds_to_sleep = calculate_seconds(int(response.headers['X-RateLimit-Reset']))

        if seconds_to_sleep > MAX_SLEEP_SECONDS:
            message = "API rate limit exceeded, please try after {} seconds.".format(seconds_to_sleep)
            raise RateLimitExceeded(message) from None

        logger.info("API rate limit exceeded. Tap will retry the data collection after %s seconds.", seconds_to_sleep)
        time.sleep(seconds_to_sleep)

# pylint: disable=dangerous-default-value
# during 'Timeout' error there is also possibility of 'ConnectionError',
# hence added backoff for 'ConnectionError' too.
@backoff.on_exception(backoff.expo, (requests.Timeout, requests.ConnectionError), max_tries=5, factor=2)
def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url, timeout=get_request_timeout())
        if resp.status_code != 200:
            raise_for_error(resp, source)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        rate_throttling(resp)
        if resp.status_code == 404:
            # return an empty response body since we're not raising a NotFoundException
            resp._content = b'{}' # pylint: disable=protected-access
        return resp

def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def generate_pr_commit_schema(commit_schema):
    pr_commit_schema = commit_schema.copy()
    pr_commit_schema['properties']['pr_number'] = {
        "type":  ["null", "integer"]
    }
    pr_commit_schema['properties']['pr_id'] = {
        "type": ["null", "string"]
    }
    pr_commit_schema['properties']['id'] = {
        "type": ["null", "string"]
    }

    return pr_commit_schema

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path, encoding='utf-8') as file:
            schemas[file_raw] = json.load(file)

    schemas['pr_commits'] = generate_pr_commit_schema(schemas['commits'])
    return schemas

class DependencyException(Exception):
    pass

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


def write_metadata(mdata, values, breadcrumb):
    mdata.append(
        {
            'metadata': values,
            'breadcrumb': breadcrumb
        }
    )

def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    #mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])

    for field_name in schema['properties'].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_all_repos(organizations: list) -> list:
    """
    Retrieves all repositories for the provided organizations and
        verifies basic access for them.

    Docs: https://docs.github.com/en/rest/reference/repos#list-organization-repositories
    """
    repos = []

    for org_path in organizations:
        org = org_path.split('/')[0]
        for response in authed_get_all_pages(
            'get_all_repos',
            'https://api.github.com/orgs/{}/repos?sort=created&direction=desc'.format(org)
        ):
            org_repos = response.json()

            for repo in org_repos:
                repo_full_name = repo.get('full_name')

                logger.info("Verifying access of repository: %s", repo_full_name)
                verify_repo_access(
                    'https://api.github.com/repos/{}/commits'.format(repo_full_name),
                    repo
                )

                repos.append(repo_full_name)

    return repos

def extract_repos_from_config(config: dict ) -> list:
    """
    Extracts all repositories from the config and calls get_all_repos()
        for organizations using the wildcard 'org/*' format.
    """
    repo_paths = list(filter(None, config['repository'].split(' ')))

    orgs_with_all_repos = list(filter(lambda x: x.split('/')[1] == '*', repo_paths))

    if orgs_with_all_repos:
        # remove any wildcard "org/*" occurrences from `repo_paths`
        repo_paths = list(set(repo_paths).difference(set(orgs_with_all_repos)))

        # get all repositores for an org in the config
        all_repos = get_all_repos(orgs_with_all_repos)

        # update repo_paths
        repo_paths.extend(all_repos)

    return repo_paths

def verify_repo_access(url_for_repo, repo):
    try:
        authed_get("verifying repository access", url_for_repo)
    except NotFoundException:
        # throwing user-friendly error message as it checks token access
        message = "HTTP-error-code: 404, Error: Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository.".format(repo)
        raise NotFoundException(message) from None

def verify_access_for_repo(config):

    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token, 'per_page': '1', 'page': '1'})

    repositories = extract_repos_from_config(config)

    for repo in repositories:
        logger.info("Verifying access of repository: %s", repo)

        url_for_repo = "https://api.github.com/repos/{}/commits".format(repo)

        # Verifying for Repo access
        verify_repo_access(url_for_repo, repo)

def do_discover(config):
    verify_access_for_repo(config)
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_all_teams(schemas, repo_path, state, mdata, _start_date):
    org = repo_path.split('/')[0]
    with metrics.record_counter('teams') as counter:
        for response in authed_get_all_pages(
                'teams',
                'https://api.github.com/orgs/{}/teams?sort=created_at&direction=desc'.format(org)
        ):
            teams = response.json()
            extraction_time = singer.utils.now()

            for r in teams:
                team_slug = r.get('slug')
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas['teams'], metadata=metadata.to_map(mdata['teams']))
                singer.write_record('teams', rec, time_extracted=extraction_time)
                counter.increment()

                if schemas.get('team_members'):
                    for team_members_rec in get_all_team_members(team_slug, schemas['team_members'], repo_path, state, mdata['team_members']):
                        singer.write_record('team_members', team_members_rec, time_extracted=extraction_time)

                if schemas.get('team_memberships'):
                    for team_memberships_rec in get_all_team_memberships(team_slug, schemas['team_memberships'], repo_path, state, mdata['team_memberships']):
                        singer.write_record('team_memberships', team_memberships_rec, time_extracted=extraction_time)

    return state

def get_all_team_members(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    with metrics.record_counter('team_members') as counter:
        for response in authed_get_all_pages(
                'team_members',
                'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
            team_members = response.json()
            for r in team_members:
                r['_sdc_repository'] = repo_path
                r['team_slug'] = team_slug

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()

                yield rec

    return state

def get_all_team_memberships(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    for response in authed_get_all_pages(
            'team_members',
            'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
        team_members = response.json()
        with metrics.record_counter('team_memberships') as counter:
            for r in team_members:
                username = r['login']
                for res in authed_get_all_pages(
                        'memberships',
                        'https://api.github.com/orgs/{}/teams/{}/memberships/{}'.format(org, team_slug, username)
                ):
                    team_membership = res.json()
                    team_membership['_sdc_repository'] = repo_path
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(team_membership, schemas, metadata=metadata.to_map(mdata))
                    counter.increment()
                    yield rec
    return state


def get_all_issue_events(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "issue_events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0


    with metrics.record_counter('issue_events') as counter:
        for response in authed_get_all_pages(
                'issue_events',
                'https://api.github.com/repos/{}/issues/events?sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for event in events:
                event['_sdc_repository'] = repo_path
                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = event.get('created_at') if event.get('updated_at') is None else event.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(event, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state


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

def get_all_issue_milestones(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `due on` ??? confirm.
    # https://developer.github.com/v3/issues/milestones/#list-milestones-for-a-repository
    # 'https://api.github.com/repos/{}/milestones?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "issue_milestones", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('issue_milestones') as counter:
        for response in authed_get_all_pages(
                'milestones',
                'https://api.github.com/repos/{}/milestones?direction=desc'.format(repo_path)
        ):
            milestones = response.json()
            extraction_time = singer.utils.now()
            for r in milestones:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and r.get("due_on") and singer.utils.strptime_to_utc(r.get("due_on")) < bookmark_time:
                    continue

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_milestones', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_milestones', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issue_labels(schemas, repo_path, state, mdata, _start_date):
    # https://developer.github.com/v3/issues/labels/
    # not sure if incremental key
    # 'https://api.github.com/repos/{}/labels?sort=created_at&direction=desc'.format(repo_path)

    with metrics.record_counter('issue_labels') as counter:
        for response in authed_get_all_pages(
                'issue_labels',
                'https://api.github.com/repos/{}/labels'.format(repo_path)
        ):
            issue_labels = response.json()
            extraction_time = singer.utils.now()
            for r in issue_labels:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_labels', rec, time_extracted=extraction_time)
                counter.increment()

    return state

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
                    { 'Accept': 'application/vnd.github.inertia-preview+json' }
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
                        rec = transformer.transform(r, schemas['projects'], metadata=metadata.to_map(mdata['projects']))
                    singer.write_record('projects', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'projects', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()

                    project_id = r.get('id')

                    # sync project_columns if that schema is present (only there if selected)
                    if schemas.get('project_columns'):
                        for project_column_rec in get_all_project_columns(project_id, schemas['project_columns'], repo_path, state, mdata['project_columns'], start_date):
                            singer.write_record('project_columns', project_column_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'project_columns', {'since': singer.utils.strftime(extraction_time)})

                            # sync project_cards if that schema is present (only there if selected)
                            if schemas.get('project_cards'):
                                column_id = project_column_rec['id']
                                for project_card_rec in get_all_project_cards(column_id, schemas['project_cards'], repo_path, state, mdata['project_cards'], start_date):
                                    singer.write_record('project_cards', project_card_rec, time_extracted=extraction_time)
                                    singer.write_bookmark(state, repo_path, 'project_cards', {'since': singer.utils.strftime(extraction_time)})
        except GithubException:
            logger.info("Projects are disabled on this repo %s. Continuing the process...", repo_path)
    return state


def get_all_project_cards(column_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_cards", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('project_cards') as counter:
        for response in authed_get_all_pages(
                'project_cards',
                'https://api.github.com/projects/columns/{}/cards?sort=created_at&direction=desc'.format(column_id)
        ):
            project_cards = response.json()
            for r in project_cards:
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
                counter.increment()

    return state

def get_all_pull_requests(schemas, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''

    bookmark_value = get_bookmark(state, repo_path, "pull_requests", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('pull_requests') as counter:
        with metrics.record_counter('reviews') as reviews_counter:
            for response in authed_get_all_pages(
                    'pull_requests',
                    'https://api.github.com/repos/{}/pulls?state=all&sort=updated&direction=desc'.format(repo_path)
            ):
                pull_requests = response.json()
                extraction_time = singer.utils.now()
                for pr in pull_requests:


                    # skip records that haven't been updated since the last run
                    # the GitHub API doesn't currently allow a ?since param for pulls
                    # once we find the first piece of old data we can return, thanks to
                    # the sorting
                    if bookmark_time and singer.utils.strptime_to_utc(pr.get('updated_at')) < bookmark_time:
                        return state

                    pr_num = pr.get('number')
                    pr_id = pr.get('id')
                    pr['_sdc_repository'] = repo_path
                    pr_detail = get_pr_detail(pr_num, repo_path)
                    pr = enhance_pull(pr, pr_detail)

                    # transform and write pull_request record
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(pr, schemas['pull_requests'], metadata=metadata.to_map(mdata['pull_requests']))
                    singer.write_record('pull_requests', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'pull_requests', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()

                    # sync reviews if that schema is present (only there if selected)
                    if schemas.get('reviews'):
                        for review_rec in get_reviews_for_pr(pr_num, schemas['reviews'], repo_path, state, mdata['reviews'], pr_id):
                            singer.write_record('reviews', review_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'reviews', {'since': singer.utils.strftime(extraction_time)})

                            reviews_counter.increment()

                    # sync review comments if that schema is present (only there if selected)
                    if schemas.get('review_comments'):
                        for review_comment_rec in get_review_comments_for_pr(pr_num, schemas['review_comments'], repo_path, state, mdata['review_comments']):
                            singer.write_record('review_comments', review_comment_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'review_comments', {'since': singer.utils.strftime(extraction_time)})

                    if schemas.get('pr_commits'):
                        for pr_commit in get_commits_for_pr(
                                pr_num,
                                pr_id,
                                schemas['pr_commits'],
                                repo_path,
                                state,
                                mdata['pr_commits']
                        ):
                            singer.write_record('pr_commits', pr_commit, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'pr_commits', {'since': singer.utils.strftime(extraction_time)})

    return state

def get_pr_detail(pull_number, repo_path):
    pr = authed_get(
        'pull',
        f'https://api.github.com/repos/{repo_path}/pulls/{pull_number}'
    )
    return pr.json()


def enhance_pull(pr_data, pr_detail_data):
    pr_data['additions'] = pr_detail_data["additions"]
    pr_data['deletions'] = pr_detail_data["deletions"]
    pr_data['comments'] = pr_detail_data["comments"]
    pr_data['review_comments'] = pr_detail_data["review_comments"]
    pr_data['commits'] = pr_detail_data["commits"]
    pr_data['changed_files'] = pr_detail_data["changed_files"]
    pr_data['merged_by'] = pr_detail_data["merged_by"]
    pr_data['base'] = pr_detail_data["base"]
    pr_data['head'] = pr_detail_data["head"]
    pr_data['user'] = pr_detail_data["user"]
    pr_data['milestone'] = pr_detail_data["milestone"]
    pr_data['assignee'] = pr_detail_data["assignee"]
    pr_data['assignees'] = pr_detail_data["assignees"]
    pr_data['requested_reviewers'] = pr_detail_data["requested_reviewers"]
    pr_data['requested_teams'] = pr_detail_data["requested_teams"]
    pr_data['requested_reviewers'] = pr_detail_data["requested_reviewers"]
    pr_data['url'] = pr_detail_data["url"]
    return pr_data


def get_reviews_for_pr(pr_number, schema, repo_path, state, mdata, pr_id):
    for response in authed_get_all_pages(
            'reviews',
            'https://api.github.com/repos/{}/pulls/{}/reviews'.format(repo_path,pr_number)
    ):
        reviews = response.json()
        for review in reviews:
            review['_sdc_repository'] = repo_path
            review['pull_request_id'] = str(pr_id)
            review['pull_request_number'] = pr_number
            with singer.Transformer() as transformer:
                rec = transformer.transform(review, schema, metadata=metadata.to_map(mdata))
            yield rec

        return state

def get_review_comments_for_pr(pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'comments',
            'https://api.github.com/repos/{}/pulls/{}/comments'.format(repo_path,pr_number)
    ):
        review_comments = response.json()
        for comment in review_comments:
            comment['_sdc_repository'] = repo_path
            with singer.Transformer() as transformer:
                rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
            yield rec


        return state

def get_commits_for_pr(pr_number, pr_id, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'pr_commits',
            'https://api.github.com/repos/{}/pulls/{}/commits'.format(repo_path,pr_number)
    ):

        commit_data = response.json()
        for commit in commit_data:
            commit['_sdc_repository'] = repo_path
            commit['pr_number'] = pr_number
            commit['pr_id'] = pr_id
            commit['id'] = '{}-{}'.format(pr_id, commit['sha'])
            with singer.Transformer() as transformer:
                rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
            yield rec

        return state


def get_all_assignees(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees
    '''
    with metrics.record_counter('assignees') as counter:
        for response in authed_get_all_pages(
                'assignees',
                'https://api.github.com/repos/{}/assignees'.format(repo_path)
        ):
            assignees = response.json()
            extraction_time = singer.utils.now()
            for assignee in assignees:
                assignee['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(assignee, schema, metadata=metadata.to_map(mdata))
                singer.write_record('assignees', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_all_collaborators(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    with metrics.record_counter('collaborators') as counter:
        try:
            responses = authed_get_all_pages(
                    'collaborators',
                    'https://api.github.com/repos/{}/collaborators'.format(repo_path)
                )
        except NotFoundException as error:
            logger.info(
                'Unable to retreive collaborators stream, check access_token is valid for %s. See full error message: %s',
                    repo_path, error
                )
        else:
            for response in responses:
                collaborators = response.json()
                extraction_time = singer.utils.now()
                for collaborator in collaborators:
                    collaborator['_sdc_repository'] = repo_path
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(collaborator, schema, metadata=metadata.to_map(mdata))
                    singer.write_record('collaborators', rec, time_extracted=extraction_time)
                    counter.increment()

    return state

def get_all_commits(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    bookmark = get_bookmark(state, repo_path, "commits", "since", start_date)
    if bookmark:
        query_string = '?since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('commits') as counter:
        for response in authed_get_all_pages(
                'commits',
                'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)
        ):
            commits = response.json()
            extraction_time = singer.utils.now()
            for commit in commits:
                commit['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                singer.write_record('commits', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'commits', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issues(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''

    bookmark = get_bookmark(state, repo_path, "issues", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages(
                'issues',
                'https://api.github.com/repos/{}/issues?state=all&sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            issues = response.json()
            extraction_time = singer.utils.now()
            for issue in issues:
                issue['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(issue, schema, metadata=metadata.to_map(mdata))
                singer.write_record('issues', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issues', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_comments(schema, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/comments/#list-comments-in-a-repository
    '''

    bookmark = get_bookmark(state, repo_path, "comments", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('comments') as counter:
        for response in authed_get_all_pages(
                'comments',
                'https://api.github.com/repos/{}/issues/comments?sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            comments = response.json()
            extraction_time = singer.utils.now()
            for comment in comments:
                comment['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
                singer.write_record('comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_stargazers(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers
    '''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}

    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages(
                'stargazers',
                'https://api.github.com/repos/{}/stargazers'.format(repo_path), stargazers_headers
        ):
            stargazers = response.json()
            extraction_time = singer.utils.now()
            for stargazer in stargazers:
                user_id = stargazer['user']['id']
                stargazer['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(stargazer, schema, metadata=metadata.to_map(mdata))
                rec['user_id'] = user_id
                singer.write_record('stargazers', rec, time_extracted=extraction_time)
                counter.increment()

    return state


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
                run = enhance_workflow_run(run, repo_path)
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


def get_commit_detail(repo_path, commit_id):
    commit = authed_get(
        'pull',
        f'https://api.github.com/repos/{repo_path}/commits/{commit_id}'
    )
    return commit.json()


def enhance_workflow_run_with_commit_info(run, repo_path):
    commit_id = run["head_commit"]["id"]
    commit = get_commit_detail(repo_path, commit_id)
    run["head_commit_author_id"] = commit["author"]["id"] if commit["author"] else ""
    run["head_commit_author_login"] = commit["author"]["login"] if commit["author"] else ""
    run["head_commit_author_email"] = commit["commit"]["author"]["email"] if commit["commit"] else ""
    return run


def enhance_workflow_run(run, repo_path):
    run = enhance_workflow_run_with_commit_info(run, repo_path)
    return run


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_id:
            return stream
    return None

# return the 'timeout'
def get_request_timeout():
    args = singer.utils.parse_args([])
    # get the value of request timeout from config
    config_request_timeout = args.config.get('request_timeout')

    # only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
    if config_request_timeout and float(config_request_timeout):
        # return the timeout from config
        return float(config_request_timeout)

    # return default timeout
    return REQUEST_TIMEOUT

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

def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    start_date = config['start_date'] if 'start_date' in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    repositories = extract_repos_from_config(config)

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
                    stream_mdata = {stream_id: mdata}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                            stream_schemas[sub_stream_id] = sub_stream['schema']
                            stream_mdata[sub_stream_id] =  sub_stream['metadata']
                            singer.write_schema(sub_stream_id, sub_stream['schema'],
                                                sub_stream['key_properties'])

                    # sync stream and it's sub streams
                    state = sync_func(stream_schemas, repo, state, stream_mdata, start_date)

                singer.write_state(state)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # get optional config key `max_sleep_seconds`
    config_max_sleep = args.config.get('max_sleep_seconds')

    # set global `MAX_SLEEP_SECONDS` for rate_throttling function or use default
    global MAX_SLEEP_SECONDS #pylint: disable=global-statement
    MAX_SLEEP_SECONDS = config_max_sleep if config_max_sleep else DEFAULT_SLEEP_SECONDS

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
