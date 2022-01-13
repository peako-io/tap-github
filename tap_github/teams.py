# Python imports
# Third-Party imports
import singer
import singer.metrics as metrics
from singer import metadata
# Project imports
from tap_github.gh_client import authed_get_all_pages


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
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('teams', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'teams', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
                if schemas.get('team_members'):
                    for team_members_rec in get_all_team_members(team_slug, schemas['team_members'], repo_path, state, mdata):
                        singer.write_record('team_members', team_members_rec, time_extracted=extraction_time)
                        singer.write_bookmark(state, repo_path, 'team_members', {'since': singer.utils.strftime(extraction_time)})
                if schemas.get('team_memberships'):
                    for team_memberships_rec in get_all_team_memberships(team_slug, schemas['team_memberships'], repo_path, state, mdata):
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
