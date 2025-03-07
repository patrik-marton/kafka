import json
import logging
import os
import textwrap
import sys
from api4jenkins import Jenkins, _patch_to, Item
from github import Auth, Github
from time import time, sleep


class MatrixRun(Item):
    pass


class MavenModuleSetBuild(Item):
    pass


# patching api4jenkins with unknown classes
_patch_to('api4jenkins.build', MatrixRun)
_patch_to('api4jenkins.build', MavenModuleSetBuild)

logging.basicConfig(level='INFO')


def main():
    url = os.environ["INPUT_URL"]
    job = os.environ["INPUT_JOB"]
    username = os.environ.get("INPUT_USERNAME")
    token = os.environ.get("INPUT_TOKEN")
    parameters = os.environ.get("INPUT_PARAMETERS")
    timeout = int(os.environ.get("INPUT_TIMEOUT")) * 60
    poll_interval = int(os.environ.get("INPUT_POLL_INTERVAL"))
    gh_token = os.environ["INPUT_GITHUB_TOKEN"]
    pr_id = int(os.environ["INPUT_PR"])
    repo_name = os.environ["GITHUB_REPOSITORY"]

    gh = Github(base_url="https://github.infra.cloudera.com/api/v3", auth=Auth.Token(gh_token))

    if parameters:
        try:
            parameters = json.loads(parameters)
        except json.JSONDecodeError as e:
            raise Exception('Could not parse `parameters`') from e
    else:
        parameters = {}

    start_time = time()

    jenkins = Jenkins(url, auth=(username, token), cookies={})
    jenkins._crumb = {}  # set to empty object because the client does not handle the lack of crumb issuer in cloudera jenkins

    queue_item = jenkins.build_job(job, **parameters)
    try:
        build = wait_for_build_start(queue_item, start_time, timeout, poll_interval)
        github_comment(gh, repo_name, pr_id, f"""\
            Jenkins job started
            -
            Job: {job}
            Url: {build.url}""")
    except Exception as e:
        github_comment(gh, repo_name, pr_id, f"""\
            Failed to start Jenkins job
            -
            Job: {job}
            Error: {e}""")

    try:
        result = wait_for_build_finish(build, start_time, timeout, poll_interval)
        github_comment(gh, repo_name, pr_id, f"""\
            Jenkins job finished
            -
            Job: {job}
            Url: {build.url}
            Result: {result}""")
    except Exception as e:
        github_comment(gh, repo_name, pr_id, f"""\
            Failed waiting for Jenkins job
            -
            Job: {job}
            Url: {build.url}
            Error: {e}""")


def github_comment(gh, repo_name, pr_id, message):
    repo = gh.get_repo(repo_name)
    pr = repo.get_pull(
        number=pr_id).as_issue()  # need to convert to issue, otherwise it would be a review comment on a file
    pr.create_comment(textwrap.dedent(message))


def wait_for_build_start(queue_item, start_time, timeout, poll_interval):
    while time() - start_time < timeout:
        logging.info('Waiting for build to start...')
        build = queue_item.get_build()
        if build:
            logging.info(f"Build started: {build.url}")
            return build
        sleep(poll_interval)
    else:
        raise Exception(f"timeout after {timeout} seconds. Job might be still in queue, consider increasing the timeout of this action")


def wait_for_build_finish(build, start_time, timeout, poll_interval):
    while time() - start_time < timeout:
        logging.info('Waiting for build to finish...')
        result = build.result

        # UNSTABLE status means successful for kafka
        # because it uses retry gradle plugin which makes the build unstable in case there were failure but the retry succeeded.
        if result in ('ABORTED', 'FAILURE', 'SUCCESS', 'UNSTABLE'):
            logging.info(f"Build finished: {result}")
            return result
        sleep(poll_interval)
    else:
        raise Exception(f"timeout after {timeout} seconds. Job might be still running, consider increasing the timeout of this action")


if __name__ == "__main__":
    main()
