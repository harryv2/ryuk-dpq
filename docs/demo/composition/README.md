# Demo video composition

The [HyperFrames](https://github.com/heygen-com/hyperframes) source for
`../ryuk-demo.mp4` — a walkthrough of the UI against a live four-node cluster.

```bash
npm run check     # lint, layout, contrast
npm run render    # writes renders/<timestamp>.mp4
```

Copy the render to `../ryuk-demo.mp4`, then make the version small enough to
attach to a pull request or issue:

```bash
ffmpeg -i ../ryuk-demo.mp4 -c:v libx264 -preset slow -crf 23 -profile:v high \
  -pix_fmt yuv420p -movflags +faststart -c:a aac -b:a 128k \
  ../ryuk-demo-compressed.mp4
```

10.2 MB to 4.8 MB with no visible loss -- screen content is mostly flat colour
and sharp edges, which H.264 handles well. Only the compressed cut is committed;
the full render and `renders/` are gitignored.

`build.py` generates `index.html` from a scene list: one `.clip` per scene on a
single paused GSAP timeline, deterministic. Edit the list and re-run it rather
than hand-editing timings.

`assets/` holds the screenshots. They are captured at **1920×968**, which is the
composition's shot area — matching the aspect means the app fills the frame
instead of being letterboxed inside it.

`assets/bgm.m4a` is the bed, lifted from the audio track of the alpha-law demo
render:

```bash
ffmpeg -ss 24 -t 80 -i ../../alpha-law-test/docs/demo/demo.mp4 \
  -af "afade=t=in:st=0:d=2,afade=t=out:st=76:d=4,loudnorm=I=-20:TP=-2:LRA=11" \
  -ar 48000 -c:a aac -b:a 192k assets/bgm.m4a
```

24s in avoids the intro and stops before the section break at 135s.
