# Python imports
import argparse
import os
import json
import time
import requests
# Third-Party imports
import singer
import singer.metrics as metrics
import backoff
# Project imports
from tap_github.streams import get_catalog
from tap_github.exceptions import *
# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300
DEFAULT_SLEEP_SECONDS = 600
MAX_SLEEP_SECONDS = DEFAULT_SLEEP_SECONDS
session = requests.Session()
logger = singer.get_logger()


def raise_for_error(resp, source):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except Exception:
        response_json = {}

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, Error: {}. Please refer \'{}\' for more details.".format(details, response_json.get("documentation_url"))
    else:
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
@backoff.on_exception(
    backoff.expo,
    (RateLimitExceeded, InternalServerError, requests.ConnectionError),
    max_tries=5,
    factor=2)
def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url, timeout=get_request_timeout())
        if resp.status_code != 200:
            raise_for_error(resp, source)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        rate_throttling(resp)
        return resp


def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break


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