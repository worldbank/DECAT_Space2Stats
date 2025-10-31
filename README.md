# Space2Stats

Consistent, comparable, authoritative data describing sub-national variation is a constant point of complication for World Bank teams, our development partners, and client countries when assessing and investigating economic issues and national policy. This project will focus on creating and disseminating such data through aggregation of geospatial information at standard administrative divisions, and through the attribution of household survey data with foundational geospatial variables.

## Documentation
Examples of using the database can be found on our github pages - https://worldbank.github.io/DECAT_Space2Stats/readme.html

## Getting Started Locally
See detailed instructions on running local database versions under the dev-db folder

## Contributing

A Github Action is used to automatically build and deploy the documentation to Github Pages. To contribute to the documentation, follow the steps below:

1. Create a new branch from the latest `main`.

    ```bash
    git checkout -b new_docs
    ```

2. Make changes to the documentation, e.g: markdown files, and table of contents (`docs/_toc.yml`).
3. Build the documentation locally to ensure it renders correctly.
4. Commit, push the changes to your branch and create a pull request.

    ```bash
    git add .
    git commit -m "Update documentation"
    git push origin new_docs
    ```

The site will be updated automatically once the branch is merged to main.

Note that the sphinx build command uses the conf.py file. If we need to make changes to _conf.yml, then rebuild the conf.py file by running:

```bash
jupyter-book config sphinx docs
```
