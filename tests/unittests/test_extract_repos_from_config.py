import unittest
import tap_github


@unittest.mock.patch('tap_github.get_all_repos')
class TestExtractReposFromConfig(unittest.TestCase):

    def test_single_repo(self, mocked_get_all_repos):
        config = {'repository': 'singer-io/test-repo'}
        expected_repositories = ['singer-io/test-repo']
        self.assertEqual(expected_repositories, tap_github.extract_repos_from_config(config))

    def test_multiple_repos(self, mocked_get_all_repos):
        config = {'repository': 'singer-io/test-repo singer-io/tap-github'}
        expected_repositories = ['singer-io/test-repo', 'singer-io/tap-github']
        self.assertEqual(expected_repositories, tap_github.extract_repos_from_config(config))

    def test_org_all_repos(self, mocked_get_all_repos):
        config = {'repository': 'singer-io/test-repo test-org/*'}
        expected_repositories = [
            'singer-io/test-repo',
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
            ]
        mocked_get_all_repos.return_value = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
        ]

        self.assertEqual(expected_repositories, tap_github.extract_repos_from_config(config))
