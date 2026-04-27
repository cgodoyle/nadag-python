# NADAG API Python client

A Python client and processing toolkit for retrieving geotechnical data from the Norwegian National Database for Ground investigations (NADAG) by using its API (https://geo.ngu.no/api/features/grunnundersokelser_utvidet).

## Installation
Clone the repository and install the package using pip:

```bash
git clone https://github.com/cgodoyle/nadag-python
cd nadag-python
pip install .
``` 

or 

```bash
pip install git+https://github.com/cgodoyle/nadag-python.git

```


## Usage

## Considerations

## Versioning and releases

This repository uses [Conventional Commits](https://www.conventionalcommits.org/) together with `python-semantic-release`.

- `feat:` bumps the **minor** version
- `fix:` and `perf:` bump the **patch** version
- `BREAKING CHANGE:` triggers a breaking release
- `chore:`, `docs:`, `test:`, `ci:` and similar commits do not create a version bump

Example commit messages:

```text
feat(api): add paginated endpoint helper
fix(http): handle API timeouts during status checks
docs(readme): explain release workflow
```

Releases are created automatically by GitHub Actions on every push to `main`.
The workflow will:

1. inspect commit history since the last tag
2. calculate the next semantic version
3. update `pyproject.toml`
4. generate `CHANGELOG.md`
5. create and push the release commit and tag
6. create a GitHub release

### Local dry-run

To test the setup locally without creating a tag or pushing anything:

```bash
pip install -e .[dev]
semantic-release --noop version
```

For a more realistic local check that still avoids pushing to GitHub:

```bash
semantic-release version --no-push --no-vcs-release
```

### Notes

- The release workflow assumes the stable branch is `main`.
- If branch protection prevents `GITHUB_TOKEN` from pushing, use a Personal Access Token with `contents: write` and wire it into the workflow.
- This first setup only automates versioning, tags, changelog, and GitHub releases. Package build/publish can be added later without changing the commit convention.
- While the project remains in `0.y.z`, breaking changes will keep incrementing the **minor** digit instead of jumping to `1.0.0`.


## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing
Contributions to this project are welcome. Please fork the repository and create a pull request with your changes. For major changes, please open an issue first to discuss what you would like to change.

## Contact
For any questions or issues, please open an issue in the repository or contact me at crgo@nve.no.