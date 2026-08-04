---
name: Performance
about: Slow decode / high memory / bench regression
title: "perf: "
labels: performance
assignees: ""
---

## Symptom

- API used (`PdfEngine.decode`, `.elementsStream`, pipeline, etc.):
- Fixture (path or attach PDF):
- Observed time / memory:

## Expected

## Bench output (if any)

```
sbt "bench/Jmh/run -i 5 -wi 3 .*PdfHyperdriveBench.*"
```

## Environment

- zio-pdf version / commit:
- JVM:
