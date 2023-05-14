# Contributing to Internet Health Report

First off, thanks for taking the time to contribute! 🎉🎉

When contributing to this repository, please first discuss the change you wish to make via issue
with the maintainers of this repository before making a change. These are mostly guidelines, not
rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

## Code of Conduct

This project and everyone participating in it is governed by the [IHR Code of
Conduct](https://github.com/InternetHealthReport/ihr-website/blob/master/CODE_OF_CONDUCT.md), please
follow it in all your interaction with the project. By participating, you are expected to uphold
this code. Please report unacceptable behavior to admin@ihr.live

## Pull Request Process

1. Make sure that your code is formatted and passed linting according to the project
   requirements. This can easily be achieved by installing the `pre-commit` command as
   described below.
1. Ensure any new dependencies are added to the `requirements.txt` file.
1. Add only relevant files to the commit and ignore the rest to keep the repo clean.
    - If you add a new dataset / crawler, include a README.md describing the crawler
       and the nodes / relationships it will push to the database.
    - If you change the build process, update the general README.md if required.
1. You should request review from the maintainers once you submit the Pull Request.

## Instructions

### Git Workflow

```bash
## Step 1: Fork Repository

## Step 2: Git Set Up & Download
# Clone the repo
git clone https://github.com/<User-Name>/<Repo-Name>.git
# Add upstream remote
git remote add upstream https://github.com/InternetHealthReport/internet-yellow-pages.git
# Fetch and merge with upstream/main
git fetch upstream
git merge upstream/main

## Step 3: Setup Virtual Environment and Install Dependencies
python3 -m venv --upgrade-deps .venv
source .venv/bin/activate
pip install -r requirements.txt

## Step 4: Setup pre-commit
pre-commit install

## Step 5: Create and Publish Working Branch
git checkout -b <type>/<issue|issue-number>/{<additional-fixes>}
git push origin <type>/<issue|issue-number>/{<additional-fixes>}

## Types:
# wip - Work in Progress; long term work; mainstream changes;
# feat - New Feature; future planned; non-mainstream changes;
# bug - Bug Fixes
# exp - Experimental; random experimental features;
```

### On Task Completion

```bash
## Commit and Push Your Work
# Check branch
git branch
# Fetch and merge with upstream/main
git fetch upstream
git merge upstream/main
# Add untracked files
git add .
# Commit all changes with appropriate commit message and description
git commit -m "your-commit-message" -m "your-commit-description"
# Fetch and merge with upstream/main again
git fetch upstream
git merge upstream/main
# Push changes to your forked repository
git push origin <type>/<issue|issue-number>/{<additional-fixes>}
```

Create the PR using GitHub Website.

Create a pull request from `<type>/<issue|issue-number>/{<additional-fixes>}` branch in your forked
repository to the main branch in the upstream repository.

After creating the PR, add a reviewer (any admin) and yourself as the assignee. Link the PR to
appropriate issue, or Project+Milestone (if no issue was created).

### After PR Merge

```bash
# Delete branch from forked repo
git branch -d <type>/<issue|issue-number>/{<additional-fixes>}
git push --delete origin <type>/<issue|issue-number>/{<additional-fixes>}
# Fetch and merge with upstream/main
git checkout main
git pull upstream
git push origin
```

- Always follow [commit message standards](https://chris.beams.io/posts/git-commit/)
- About the [fork-and-branch workflow](https://blog.scottlowe.org/2015/01/27/using-fork-branch-git-workflow/)
