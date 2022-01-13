# Python imports
# Third-Party imports
# Project imports

REQUIRED_CONFIG_KEYS = ['start_date', 'access_token', 'repository']

KEY_PROPERTIES = {
    'commits': ['sha'],
    'comments': ['id'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests': ['id'],
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
    'team_members': ['id'],
    'team_memberships': ['url'],
    'workflow_runs': ['id']
}