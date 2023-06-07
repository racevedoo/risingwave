# CI Workflows

We use both GitHub Actions (in `.github/workflows`) and BuilKite (in `ci/workflows`) for CI. Most of jobs including building and testing are run on BuilKite. GitHub Actions is used for some miscellaneous checks.

## BuildKite CI

https://buildkite.com/risingwavelabs lists all pipelines.

Each YAML file in `ci/workflows` defines a pipeline, which is composed of multiple steps. Each step is a job that runs on a docker container. The pipeline is triggered by a webhook from GitHub when a PR is opened or updated.

### The CI image

`rw-build-env` is a docker image containing the build tools (e.g., Rust) for RisingWave. It's not necessary for running tests.

To update the image (e.g., to update the Rust toolchain version, or to add a new build tool), you can edit the `Dockerfile`. Then update the `BUILD_ENV_VERSION` in `build-ci-image.sh` and `docker-compose.yml`. The CI will automatically check the env version and build a new image.

## Sqlsmith

Sqlsmith has two `cron` workflows.
1. Frontend tests
2. E2e tests

It is separate from PR workflow because:
1. Fuzzing tests might fail as new features and generators are added.
2. Avoid slowing down PR workflow, fuzzing tests take a while to run.
   We can include failing tests in e2e / unit tests when encountered.
