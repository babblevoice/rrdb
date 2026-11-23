# AGENTS.md

The conventions for coding agents working in rrdb, the round-robin database
server (see README.md for what it does and its commands). CLAUDE.md imports
this file, so Claude Code reads it; every other agent reads it here.

## Tests, pull requests and releases

<!-- From babblevoice/babble-configs, workflow-templates/agents/release-flow.md
(BACKEND-25). Change only what is between a ">>> settings" comment and its
"<<< settings", and fill in each CHANGE-ME. Change the rest in babble-configs
first, then here, as for the workflows. -->

This repository builds, tests and releases the way every babblevoice repository
with a Dockerfile does. Its workflows are copies of the templates in
babblevoice/babble-configs (`workflow-templates/`), and this section is a copy
too. Where it says `main`, read the repository's default branch, which is
`master` in a repository still on it.

### Run the suites before a pull request

CI runs no suite on a pull request or a merge while the repository is private.
A pull request gets the cheap checks: the workflow lint and the repository's own
lint (`pr-checks.yml`), the secret scan, the image build when a file it is built
from changes, and the check that proves the suites ran (below). The suites run
in CI on a release tag, where they gate the image (`release.yml`), and nightly
on main (`nightly.yml`, once on each commit main moves to, then not again until
it moves; a red verdict stays red until main moves or someone runs it by hand),
both through `suites.yml`. A public repository's minutes are free,
so there the suites run on every pull request as well, and the run before the
pull request is still required.

So the run on your machine is the only thing between a broken change and
everyone else's branches, and the pull request has to show it. Run every suite
on the commit you push, people and agents alike:

<!-- >>> settings: the suites, and how to run them -->
The suite is the mocha specs in `test/`, run in the Dockerfile's `dev` stage,
which has Node, libfaketime and the rrdb binary the image's `builder` stage
compiles. Each spec runs that binary under a fixed fake time and checks what it
writes and reads back.

```bash
docker build --target dev -t rrdb:dev . && docker run --rm --network none rrdb:dev npm test
```

It takes about a minute, and it builds the working tree, not the commit: run it
with nothing uncommitted, or the line names a commit that is not what ran.
mocha prints `N passing` and, if any did, `M failing`; the line says
`unit N passed, M failed`. rrdb has no lint of its own.
<!-- <<< settings -->

<!-- >>> settings: when a suite may be left out -->
No suite is left out.
<!-- <<< settings -->

### The line that proves the run

The pull request's test plan carries one item, written from the suites' own
summaries, naming the commit they ran on:

```markdown
- [x] Suites run on 1a2b3c4d: unit 412 passed, 0 failed; browser 96 passed, 0 failed
```

Each suite gives its counts in these words, `passed` and `failed`, whatever its
runner prints (mocha says `passing`, and leaves out a `failing` of 0).
`pr-proof.yml` fails the pull request when there is no such item, when it names
a commit other than the pull request's head, or when a suite on it does not
give both counts or failed any. An example in a code block or an HTML comment is
not read as the item. Every new commit on the branch needs a new run and a new
line. GitHub's "Update branch" makes a merge commit nobody has run anything on,
so pull it, run the suites on it and update the line, or merge main locally and
run them before you push. Name only the suites that ran, and never write one as
passed that did not run. The check catches forgetting, not lying: it reads the
line, never the run.

### A workflow-only change is the one exception

A pull request that changes only the CI workflows, and the notes about them,
runs the templates' copy check and actionlint instead of the suites, and says
so. The copy check is run from an up-to-date checkout of babble-configs'
master:

```bash
../babble-configs/workflow-templates/check-copy.sh .
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.12
```

```markdown
- [x] Suites not run: workflow-only change; check-copy.sh and actionlint passed
```

A pull request that changes anything else as well runs the suites. The check
only knows whether something under `.github/workflows/` changed, and refuses
the line from a pull request that changes nothing there, so a change to this
section alone runs the suites too.

### Releases

A release is a tag, and nothing else publishes an image: not a merge, not a
pull request. `release.yml` checks the tag, runs the suites on the tagged
commit, builds the image on each platform, starts it and checks it is still up
(or answers), and only then pushes it, under its exact version and nothing
else: no `latest`, `stable`, `1.4`, `1`, `sha-…` or `pr-…`. Then it makes the
GitHub release. A full release is built again from its own tag, never promoted
from an rc's image.

```bash
git tag v1.4.0-rc.1 && git push origin v1.4.0-rc.1   # a pre-release, to try out
git tag v1.4.0 && git push origin v1.4.0             # a full release
```

<!-- >>> settings: where a release goes -->
rrdb runs in one place: babble-configs' dbstack (`dbstack/compose.yaml`), which
cold, warm and hot all reach as `rrdbserver`. There is no cold or warm copy to
try a release on, so a pre-release is tried locally, against a copy of the data
if the change touches the file format, and a full release goes live by changing
dbstack's pin.
<!-- <<< settings -->

- **A pre-release is `vX.Y.Z-rc.N`, and anyone with write access may cut
  one**, from main, or for a hotfix from its `hotfix/X.Y` branch. Its notes
  start at the release before it, rc or not.
- **A full release is `vX.Y.Z`, created by the Operations Director or someone
  they have named, and creating it is the approval for it to go live.** That is
  the written process; GitHub enforces nothing. A release `release.yml` makes
  is GitHub's bot's, so its notes open with who pushed the tag; one made by hand
  shows who made it. Its notes start at the previous full release, so 1.2.0
  lists everything since 1.1.1, its rcs included.
- **A release made by hand on the Releases page works too**, and its notes are
  kept as written: set its Previous tag, since GitHub does not document what
  "auto" picks. Save it as a draft and publish it once the run has published
  the image, or the page announces a version that may never exist.
- **An agent creates or pushes an rc tag only when asked to in that
  conversation, and never a full release.**
- **The version is classic semver, chosen by whoever tags:** major for a large
  change, minor for a feature, patch for a fix. Any other tag is refused before
  anything is built, a `-beta` included.
- **A tag never moves, and a version is published once.** One already on Docker
  Hub fails the gate before anything is built. A run that fails before
  publishing can be re-run when the cause was not the code (a flaky suite,
  Docker Hub down); a fix to the code is a new commit and the next number. A
  version that was published and then found broken stays used, and the next
  one skips its number.

<!-- >>> settings: going back a version -->
**dbstack must still run if it goes back one version.** A broken release is
rolled back by pinning the version before it, so that version has to read and
write the data files the new one leaves behind in `/var/rrdb` (the
`dbrrdbdata` volume). A release that can't allow that (a change to the file
format) says so at the top of its release notes, with the way back.
<!-- <<< settings -->

### Hotfixes

For a security problem, an outage or data at risk, and nothing else, a patch
can go out without everything else on main. The fix merges to main first. Then
it is copied with `git cherry-pick -x`, whose line `(cherry picked from commit
…)` names the commit on main, and reaches `hotfix/X.Y` through a pull request
like any other, with its own proof line. The branch is made from the release's
tag the first time it is needed. Then it is tagged there (`v1.1.2`).

```bash
git fetch origin
git switch -c hotfix/1.1 v1.1.1 && git push -u origin hotfix/1.1   # the first time
git switch -c hotfix-1.1-outage origin/hotfix/1.1 && git cherry-pick -x <commit on main>
```

- **The branch you work on is named with a dash, never under `hotfix/`.**
  `release.yml` reads every `hotfix/*` branch as a release line, so a working
  branch there would hold back every release from main.
- **A tag older than these workflows has the old ones.** A tag runs the
  workflows at its own commit, so a hotfix branch made from a release cut before
  this repository took the templates would publish the old way, under the old
  tags and gates. The first pull request into such a branch copies, with
  `git cherry-pick -x`, the commit that took them and each later change to
  them, before anything else goes in or is tagged. These are the commits on
  main that changed the workflows since, oldest first. One that changed code as
  well would bring the code with it: copy only its workflows, and write its
  `-x` line by hand.

  ```bash
  first=$(git log --format=%h --diff-filter=A origin/main \
    -- .github/workflows/suites.yml | tail -1)
  git log --reverse --format='%h %s' "$first^..origin/main" -- .github/workflows/
  ```

- **A hotfix may go straight to a full release**, tried first where the
  section above says, and is approved like any other. An rc of a hotfix goes
  only where the version it fixes is being tried, never ahead of main's.
- **A branch takes only its own minor's tags, and is deleted** once a newer
  minor has gone live.
- **No release from main leaves a hotfix behind.** While any hotfix branch
  holds a change main has no copy of, `release.yml` refuses every rc and full
  release from main, so in an emergency the hotfix can go first and still
  cannot be lost from the next release. A copy is matched by an identical patch
  or by its line, read either way round: a copy that had to be written
  differently keeps the line, written by hand if `-x` did not.

## rrdb's older releases

Every tag up to `v1.2.3` was made before this repository took the templates, so
it runs the old `build.yaml`: on a tag that publishes `X.Y`, `X`, `latest` and a
`sha-` tag beside the version, with no tests and no start-up check. A hotfix
branch made from one of them starts with the copy described under Hotfixes,
above. Nothing moves Docker Hub's `latest`, `1` and `1.2` any more, so they stay
on 1.2.3: pin a version, never one of those.
