# Kafka Visual Lab design direction

Written in the design pass (step 5b of the visual-lab skill). Every lab has its own art direction.

## Subject and audience
- Apache Kafka: a distributed commit log. Its world is the append-only log itself: partitions as
  ordered tapes of records, offsets as line numbers, consumers as cursors moving along them. The
  Apache Kafka mark is plain black on white.
- Visitors are developers who have heard "Kafka is a log" and want to watch records land,
  fan out to consumer groups, retry and end up in a dead-letter topic.
- The UI's job: one screen where every stage of a record's trip lights up as it happens.

## Candidates considered
- `ui-ux-pro-max` (`"Apache Kafka event streaming log broker developer learning lab"
  --design-system`) suggested "Swiss Modernism 2.0": navy #1E3A5F, gold #A16207, EB Garamond.
- Rejected: the same academic result came back for every tool queried; nothing in it is about
  logs or streams.
- Kept: strict grid, 4.5:1 contrast, visible focus, reduced motion.
- The old look (slate #0f172a page, sky-blue title, five candy-coloured buttons, glowing halos)
  was the generic dark dashboard.

## Direction (one sentence)
"Highlighted log": the lab is a printed commit log, black ink on cool paper, partitions as ruled
tape, and a yellow highlighter marking the newest offset in each partition, the newest
telemetry line and the stage a record is in right now.

## Tokens
| Role | Value | Notes |
|---|---|---|
| Background `--bg` | #eceeed | cool paper (not cream); flat |
| Panels `--panel`, `--panel-2` | #ffffff, #f5f6f5 | tape `--tape` #f8f9f8; `--border` #cfd3d1, `--rule` #e1e4e2 |
| Text `--text`, `--muted` | #111417, #5a6066 | muted 5.5:1 on `--bg`, 6.4:1 on panels |
| Accent `--hl` | #ffe14d | highlighter yellow, only ever under ink text (14.2:1); `--hl-soft` #fff3b0 for wider areas |
| Header band | #111417 | the Kafka mark's black; text #f4f5f4, secondary #a7adb3 (8.2:1) |
| Stage roles | producer #1f5fbf, broker #111417, consumer A #1d7a3e, consumer B #0f6f7a, retry #8a5a00, DLT #c0262d | one per telemetry stage; all at least 4.6:1 on paper as text |
| Status ok / bad / warn | consumer A green / DLT red / retry amber | the stage colours double as status; every stage is also written out in the log |

## Type
- Display and body: Hanken Grotesk Variable (`@fontsource-variable/hanken-grotesk@5.3.0`).
  Header at weight 800, tracking -0.03em; panel titles 750; body 1rem / 1.55.
- Mono: Martian Mono Variable (`@fontsource-variable/martian-mono@5.3.0`, `wdth.css`) at 87.5%
  width, only for offsets, keys, payloads, stage names and serializer settings: the data in the log.
- Scale: h1 1.55rem (1.25rem on phones), panel titles 1–1.15rem, body 0.9–1rem, mono 0.7–0.75rem.
  Tabular figures on offsets.

## Background and surfaces
- `--page-bg` is flat paper. Partitions are ruled "tape" strips; records are small cells.
- Radii 3–6px. No glows: an active node gets an ink border and a highlighter wash instead of a
  coloured halo.
- The header is the one bold element: a black band with a yellow broker pill.

## Motion personality
- The live path is the one moving thing: when a record is in flight, the arrow between the
  stages becomes black-and-yellow tape marching in the direction of travel (it marches down on
  phones, where the stages stack).
- New offset cells and log lines pop in once. No GSAP: every motion answers a live SSE event.
- `prefers-reduced-motion` stops every animation and transition.

## Generic defaults avoided
- Dark slate dashboard with a cyan title and rainbow buttons: replaced with ink, paper and one
  highlighter.
- Coloured left bars on log rows and the lesson banner: replaced with a stage dot in the stage's
  colour and full borders.
- Glowing box-shadow halos on active nodes: replaced with an ink border and a highlighter wash.
- Title Case headings and buttons: sentence case, same facts.
- Monospace as decoration: mono only where the content is log data.

## Audit log
- 2026-11-21 `impeccable detect` (regex scan of `src`): 0 findings after the rewrite (the old CSS
  had left-bar log rows and a left-bar banner; both removed). URL mode could not run: no Chromium
  on the host.
  - Craft floor: themed `::selection` (highlighter) and scrollbars; `transition` lists properties.
- 2026-11-21 `web-design-guidelines` on `src/**/*.{jsx,css}`: all controls are `<button>`s, no
  clickable divs, `:focus-visible` ring added, the in-flight line is `aria-live="polite"`,
  "..." replaced with a full sentence, Title Case to sentence case, `theme-color`, description
  and a favicon added (there was none), `touch-action: manipulation`.
  - Not changed: the "KRaft broker, 3 partitions" pill is static text, not a live connection
    check (same as before; it no longer claims "Connected").
- 2026-11-21 `screenshot.sh`: the stock script waits for network idle, which never happens while
  the SSE stream is open; ran a copy that waits for `load`: 2 screenshots (1440 and 390), 0
  problems. Then a scripted run that clicks all five scenarios against the real broker and shoots
  the live state: 0 console errors, 0 overflow at 390px. Looking at it: the 5-column topology
  had no phone layout before; it now stacks with vertical marching arrows.
- 2026-11-21 `test-isolated.sh`: security and design checks pass, `GET /` ok; `GET /api/health`
  fails because this backend has no health route and the lab has no demo catalog to smoke-test.
  The real endpoints were exercised instead by the scripted screenshot run (five scenarios,
  records visible in every partition, the retry path and the DLT).
