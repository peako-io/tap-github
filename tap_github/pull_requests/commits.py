# Python imports
# Third-Party imports
import singer
# Project imports


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
