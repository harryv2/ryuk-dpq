# Making the demo video

Everything needed to rebuild `ryuk-demo.mp4` from scratch. The scenario is
scripted, so a rerun produces the same walkthrough against fresh data.

## 1. A clean cluster with background context

The walkthrough creates its own queue, but the list and cluster pages look empty
without other queues alongside it.

```bash
cd deploy
docker compose down -v && rm -rf ../data/nodes ../data/postgres ../data/etcd ../data/prometheus
docker compose up -d --scale node=4
```

Then seed two queues that are not the subject of the demo — one single-node, one
distributed — with a few dozen messages across mixed priorities and groups.

## 2. Capture

Three scripts run in order, each continuing the frame numbering of the last:

| Script | Frames | What it captures |
|---|---|---|
| `capture.js` | 001–017 | cluster, create, send four, poll through ack and nack |
| `capture-metrics.js` | 018–041 | the metrics page photographed every 5s while traffic runs |
| `capture-cluster.js` | 042–063 | distribution, scaling to six, killing a node, tenancy |

They drive real Chrome through Playwright, clicking the way a person would. Two things matter:

- **Viewport 1920×968 at `deviceScaleFactor: 2`.** That is the composition's shot
  area exactly. Any other aspect letterboxes the app inside the frame with black
  bars either side.
- **Selectors must be exact.** `:has-text()` is a substring match, so
  `button:has-text("Ack")` also matches **N-ack**, and Nack comes first in the
  DOM. Use `:text-is("Ack")`. Likewise `button:has-text("Send")` matches the
  Send **tab**, not the submit button — scope it to `form button.primary`.

```bash
npm i playwright-core
node capture.js composition/assets
node capture-metrics.js composition/assets
node capture-cluster.js composition/assets
```

### The scenario

Create a queue, send 2 HIGH / 1 MEDIUM / 1 LOW, then poll one at a time:

| Step | What the frame must show |
|---|---|
| send 4 | ready 4 |
| poll | ready 3, in flight 1, first HIGH, try 1 |
| ack | in flight 0, ready still 3 |
| poll | ready 2, second HIGH, try 1 |
| nack | ready back **up** to 3 |
| poll | same message, **try 2** |
| poll | MEDIUM, then LOW |
| drained | ready 0, in flight 0 |

Check the counters across the sequence before rendering. If an "ack" frame shows
ready going *up*, the selector hit Nack.

### Real-time metrics

`capture-metrics.js` is the part that has to be driven, not staged. It sends
faster than it acknowledges for a minute — the backlog climbs past six hundred —
then flips the ratio and drains it, photographing the page every five seconds
throughout. Played back at ~1s a frame it reads as a time-lapse.

Frames in the middle of a run carry no caption, so the band from the frame
before stays up and the run reads as one continuous shot rather than a slideshow.

## 3. Build and render

```bash
cd docs/demo/composition
python3 build.py     # scene list -> index.html
npm run check        # lint, layout, contrast — must be 0 errors
npm run render
```

`build.py` owns the scene list and timings. Edit the list, never `index.html`.

**Transitions.** Each scene runs `OVERLAP` seconds past its slot and the next one
fades in over the top, giving a dissolve. Without the overlap a clip ends, its
opaque fill goes with it, and the frame flashes dark before the next image
arrives — that flash between screenshots is what reads as flicker. Layering is
CSS `z-index` (ascending with scene order), not `data-track-index`, which
HyperFrames uses only for the Studio timeline and never reads at render.

To check it objectively, sample the brightness of a frame mid-transition against
the frames either side; a dip means a scene is still going dark between shots.
Short holds (1.0–1.2s) read like a recording; frames carrying an idea — the
priority order, the cluster placement — hold 3s+.

## 4. Music

Lifted from the alpha-law render, which has the bed baked into its audio track:

```bash
ffmpeg -ss 24 -t 80 -i ../../alpha-law-test/docs/demo/demo.mp4 \
  -af "afade=t=in:st=0:d=2,afade=t=out:st=76:d=4,loudnorm=I=-20:TP=-2:LRA=11" \
  -ar 48000 -c:a aac -b:a 192k assets/bgm.m4a
```

24s in clears the intro; 80s ends before the section break at 135s. Placed at
`data-volume="0.42"`, which lands around −27 dB mean in the render.

## 5. Publish

```bash
cp renders/<timestamp>.mp4 ../ryuk-demo.mp4
ffmpeg -i ../ryuk-demo.mp4 -c:v libx264 -preset slow -crf 29 -profile:v high \
  -pix_fmt yuv420p -movflags +faststart -c:a aac -b:a 112k \
  ../ryuk-demo-compressed.mp4
```

27.6 MB to 7.8 MB. Screen content is flat colour and sharp edges, so even CRF 29
leaves UI text and caption type indistinguishable from the source. Only the
compressed cut is committed. GitHub will not play a video from a
repository path — drag it into an issue or release for the `user-attachments`
URL that renders inline.

## Checks before shipping

- `npm run check` reports 0 errors (a lint error silently switches off the
  layout and contrast audits, so "0 samples" means nothing ran)
- Extract a frame and confirm the app fills the width with no black bars
- Confirm the caption on each frame matches what is actually on screen
