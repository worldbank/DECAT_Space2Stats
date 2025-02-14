# Development Guide for Space2StatsClient

This document provides guidelines for developers working on the Space2StatsClient project. It includes instructions on using tags, creating releases, and performing local builds for testing.

## Using Tags

Tags are used to mark specific points in the repository's history, typically for releases. The Space2StatsClient project uses these tags to automatically publish to PyPI when a release is created on GitHub. This github action is triggered by the `release.yml` workflow.   

1. **Create a Tag**

   Before creating a tag, ensure your working directory is clean and all changes are committed. 

   - **Lightweight Tag**: A simple tag without a message.

     ```bash
     git tag v1.0.0
     ```

2. **Push Tags to Remote**

   Push the created tag to the remote repository:

   ```bash
   git push origin v1.0.0
   ```

## Creating a Release

1. **Update Version**

    IMPORTANT: Update the version number in the `pyproject.toml` file.

2. **Create and Push a Tag**

   Follow the steps in the "Using Tags" section to create and push a tag.

3. **Create a Release on GitHub**

   - Navigate to the "Releases" section of your GitHub repository.
   - Click "Draft a new release".
   - Select the tag you created.
   - Fill in the release title and description.
   - Click "Publish release".

4. **Automated Publishing to PyPI**

   The release is automatically published to PyPI when a new tag is pushed to the repository AND a release is created on GitHub.

## Local Build for Testing

1. **Install the Package Locally**

   Install the package in editable mode for local testing:

   ```bash
   pip install -e .
   ```

2. **Test the Installation**

   Run your tests to ensure everything works as expected:

   ```bash
   pytest
   ```

## Additional Resources

- [Python Packaging Guide](https://packaging.python.org/)
- [Git Tagging](https://git-scm.com/book/en/v2/Git-Basics-Tagging)
- [GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)
- [Semantic Versioning](https://semver.org/)
