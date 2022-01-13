# Python imports
# Third-Party imports
# Project imports
from tap_github.gh_client import authed_get


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
