# Contributing to zio-pdf

**Internal Tybera repository** — canonical forge [git.tybera.net/Tybera/zio-pdf](https://git.tybera.net/Tybera/zio-pdf).  
Until the forge is healthy, open PRs on [github.com/AdrielC/zio-pdf](https://github.com/AdrielC/zio-pdf) and push to Tybera when it is back.  
No public mirror long-term; artifacts publish only to Tybera Gitea Packages (`com.tybera`).

## Clone (SSH)

Add to `~/.ssh/config`:

```
Host git.tybera.net
  HostName git.tybera.net
  User git
  IdentityFile ~/.ssh/id_ed25519_tybera
  IdentitiesOnly yes
```

Then:

```bash
git clone git@git.tybera.net:Tybera/zio-pdf.git
cd zio-pdf
```

HTTPS also works: `git clone https://git.tybera.net/Tybera/zio-pdf.git`

## Workflow

1. Branch from `main`: `git checkout -b feat/my-change`
2. Make changes; keep public API on **`PdfEngine`** / **`PdfStream`** / **`PdfIO`**.
3. Run locally:
   ```bash
   sbt test
   sbt examples/run
   sbt "bench/Jmh/compile" "benchFs2/Jmh/compile"
   ```
4. Open a pull request on Tybera → squash merge into `main`.
5. `main` is protected: **CI must pass** and **one approval** is required (no self-merge).

## Code conventions

- Scala 3, ZIO 2 — prefer `import zio.*` star imports in `zio.pdf` code.
- Avoid `throw` in library code; use `ZIO` / `Validation` / `Attempt`.
- Public byte surface: **`Chunk[Byte]`**, not `Array[Byte]`.
- Fused mmap paths stay behind **`PdfEngine`**; do not expose `PdfHyperdrive` publicly.
- Match existing naming: `decoded()` pipeline vs `decode(path)` fused collect.

## Commit messages

Short imperative subject, optional body explaining *why*:

```
feat: add PdfEngine.elementsSink for constant-memory classify
fix: digestSink Chunk typing on ZIO 2.1.25
docs: update pipeline-first README examples
```

## Releases & Maven (internal)

Coordinates: **`com.tybera` %% `zio-pdf`**.

Publish locally (requires Gitea token in env):

```bash
export GIT_USER=acasellas
export GIT_TOKEN=<gitea-token>
sbt publish
```

Consume from another Tybera project:

```scala
resolvers += "Tybera Gitea Maven" at "https://git.tybera.net/api/packages/Tybera/maven"
libraryDependencies += "com.tybera" %% "zio-pdf" % "0.2.0-RC1"
```

CI publishes automatically on `v*` tags. Add repo secret **`GIT_TOKEN`** (Gitea Actions → Secrets) with a token for `git.runner` or your user.

Tag on `main` and create a Gitea release from `CHANGELOG.md`.
