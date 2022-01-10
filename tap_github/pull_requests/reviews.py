# Python imports
# Third-Party imports
import singer
from singer import metadata
# Project imports
from gh_client import authed_get_all_pages


def get_reviews_for_pr(pr_number, schema, repo_path, state, mdata, pr_id):
    for response in authed_get_all_pages(
            'reviews',
            'https://api.github.com/repos/{}/pulls/{}/reviews'.format(repo_path, pr_number)
    ):
        reviews = response.json()
        for review in reviews:
            review['_sdc_repository'] = repo_path
            review['pull_request_id'] = pr_id
            review['pull_request_number'] = pr_number
            with singer.Transformer() as transformer:
                rec = transformer.transform(review, schema, metadata=metadata.to_map(mdata))
            yield rec
        return state
