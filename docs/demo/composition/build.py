import io, json

# Short holds so the walkthrough reads like a recording rather than a slideshow.
# The frames that carry an idea -- priority order, slot placement -- hold longer.
SCENES = [
    ("title",   6.0, None, None, None),

    ("div",     1.8, "PART 1", "The queue list", None),
    ("shot",    2.6, "01_queues.png", "QUEUES", "Depth, in-flight, oldest age, owner and placement type &mdash; per org, at a glance"),

    ("div",     1.8, "PART 2", "Create, then send four", None),
    ("shot",    1.6, "02_new_named.png", "NEW QUEUE", "A name, and one placement choice: single node for exact ordering, or spread across the cluster"),
    ("shot",    1.6, "03_created_empty.png", "EMPTY", "Created and placed on a node. Nothing ready, nothing in flight"),
    ("shot",    1.6, "04_sent_high1.png", "SEND", "First message in at HIGH &mdash; what was queued appears beside the form"),
    ("shot",    3.2, "05_sent_all_four.png", "FOUR IN", "Two HIGH, one MEDIUM, one LOW. Ready reads 4; the feed lists them newest first"),

    ("div",     1.8, "PART 3", "Poll, acknowledge, nack", None),
    ("shot",    1.6, "06_poll_ready.png", "ONE AT A TIME", "Taking a single message per poll, so the order the queue hands them out is visible"),
    ("shot",    2.8, "07_poll_high1.png", "HIGH FIRST", "Ready 4 &rarr; 3, in flight 1. The first HIGH comes out, attempt 1"),
    ("shot",    2.6, "08_ack_high1.png", "ACK", "In flight back to 0 and ready stays at 3 &mdash; acknowledged means gone, not returned"),
    ("shot",    2.4, "09_poll_high2.png", "SECOND HIGH", "Ready 3 &rarr; 2. Both HIGH messages come out before anything else"),
    ("shot",    3.0, "10_nack_high2.png", "NACK", "Ready climbs back to 3 &mdash; a nack returns the message to the queue instead of consuming it"),
    ("shot",    3.2, "11_poll_high2_again.png", "REDELIVERED", "The same message, now attempt 2. Retries are counted, and enough of them send it to the dead-letter queue"),
    ("shot",    2.6, "12_poll_medium.png", "THEN MEDIUM", "Only once both HIGH messages are done does MEDIUM get its turn"),
    ("shot",    2.6, "13_poll_low.png", "THEN LOW", "LOW last, exactly as submitted &mdash; strict priority, FIFO inside each band"),
    ("shot",    2.4, "14_drained.png", "DRAINED", "Ready 0, in flight 0. Four in, four out, one of them twice because it was nacked"),

    ("div",     1.8, "PART 4", "Metrics", None),
    ("shot",    3.0, "15_metrics_demo.png", "THROUGHPUT", "Enqueue and acknowledge rates, worked out by the gateway from the change between two collections &mdash; a node reports only totals"),
    ("shot",    2.2, "16_metrics_events.png", "DEPTH BY PRIORITY", "Stacked bands over a rolling window, summed across every machine holding a slot"),
    ("shot",    2.4, "17_metrics_filtered.png", "FILTER", "Hiding a band rescales the axis, so a small series is not flattened by a large one"),

    ("div",     1.8, "PART 5", "The cluster", None),
    ("shot",    3.2, "18_cluster.png", "PLACEMENT", "A distributed queue&rsquo;s slots spread over every node; a single-node queue is placed whole"),
    ("shot",    3.2, "19_cluster_nodes.png", "STOP ONE", "Each node shows its container id, so a failure can be triggered by hand and watched"),

    ("div",     1.8, "PART 6", "Tenancy and theme", None),
    ("shot",    2.4, "20_globex.png", "ANOTHER ORG", "The org comes from the credential, never the URL &mdash; Globex sees none of Acme&rsquo;s queues"),
    ("shot",    1.8, "21_dark_queues.png", "DARK", "Light and dark, remembered per browser"),
    ("shot",    2.2, "22_dark_cluster.png", "SAME DATA", "Nothing is redrawn for a theme; the charts and tables read the same palette"),

    ("outro",   9.0, None, None, None),
]

parts, tweens, t = [], [], 0.0
i = 0
for kind, dur, a, b, c in SCENES:
    i += 1
    sid = f"s{i:02d}"
    start, dur = round(t, 2), round(dur, 2)
    t += dur

    if kind == "title":
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0">
        <div class="fill"></div>
        <div class="card">
          <div class="inner">
            <div class="kicker" id="{sid}-k">DISTRIBUTED PRIORITY QUEUE</div>
            <div class="h1" id="{sid}-h">Ryuk</div>
            <div class="h2" id="{sid}-s">Priority 0&ndash;100, strict ordering within a group, at-least-once delivery.
              Two queue types: one machine for exact order, or slots spread across the cluster.</div>
            <div class="rows" id="{sid}-r">
              <div class="row"><span class="dot"></span><span>Rendezvous placement &mdash; <b>no coordinator, nothing elected</b></span></div>
              <div class="row"><span class="dot"></span><span>Write-ahead log per slot; a restarted node replays and rejoins</span></div>
              <div class="row"><span class="dot"></span><span>Nodes register themselves &mdash; adding one rebalances the work onto it</span></div>
            </div>
          </div>
        </div>
      </div>''')
        for el, off in [("k", 0.30), ("h", 0.55), ("s", 0.95), ("r", 1.35)]:
            tweens.append(f'tl.fromTo("#{sid}-{el}", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.6, ease: "power2.out" }}, {round(start+off,2)});')

    elif kind == "div":
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0">
        <div class="fill"></div>
        <div class="divider">
          <div>
            <div class="kicker center" id="{sid}-k">{a}</div>
            <div class="h1 center big" id="{sid}-h">{b}</div>
          </div>
        </div>
      </div>''')
        tweens.append(f'tl.fromTo("#{sid}-k", {{ autoAlpha: 0, y: 18 }}, {{ autoAlpha: 1, y: 0, duration: 0.4, ease: "power2.out" }}, {round(start+0.12,2)});')
        tweens.append(f'tl.fromTo("#{sid}-h", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.5, ease: "power2.out" }}, {round(start+0.28,2)});')

    elif kind == "shot":
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0">
        <div class="fill"></div>
        <div class="shot" data-layout-allow-overflow><img id="{sid}-i" src="assets/{a}" alt="" /></div>
        <div class="cap" id="{sid}-c"><span class="tag">{b}</span><span class="q">{c}</span></div>
      </div>''')
        tweens.append(f'tl.fromTo("#{sid}-c", {{ autoAlpha: 0, y: 22 }}, {{ autoAlpha: 1, y: 0, duration: 0.42, ease: "power2.out" }}, {round(start+0.1,2)});')
        tweens.append(f'tl.fromTo("#{sid}-i", {{ autoAlpha: 0, scale: 1.012 }}, {{ autoAlpha: 1, scale: 1, duration: 0.5, ease: "power2.out" }}, {start});')

    else:  # outro
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0">
        <div class="fill"></div>
        <div class="card">
          <div class="inner">
            <div class="kicker" id="{sid}-k">UNDER THE HOOD</div>
            <div class="h1" id="{sid}-h" style="font-size: 64px">One engine, two placement strategies</div>
            <div class="rows" id="{sid}-r">
              <div class="row"><span class="dot"></span><span><b>Priority bitmap</b> &mdash; highest non-empty band in constant time, whatever the depth</span></div>
              <div class="row"><span class="dot"></span><span><b>Group locking</b> &mdash; one message per group in flight, so order survives concurrency</span></div>
              <div class="row"><span class="dot"></span><span><b>Starvation reserve</b> &mdash; low priority still moves while urgent work saturates</span></div>
              <div class="row"><span class="dot"></span><span><b>Lease epochs</b> &mdash; a late acknowledgment cannot delete someone else's message</span></div>
              <div class="row"><span class="dot"></span><span><b>Freeze, ship, absorb</b> &mdash; rebalancing without losing a message or its order</span></div>
              <div class="row"><span class="dot"></span><span class="mono">Go &middot; gRPC &middot; PostgreSQL &middot; etcd &middot; Next.js &middot; Docker</span></div>
            </div>
          </div>
        </div>
      </div>''')
        for el, off in [("k", 0.30), ("h", 0.55), ("r", 1.00)]:
            tweens.append(f'tl.fromTo("#{sid}-{el}", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.65, ease: "power2.out" }}, {round(start+off,2)});')

total = round(t, 2)

html = f'''<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=1920, height=1080" />
    <script src="https://cdn.jsdelivr.net/npm/gsap@3.14.2/dist/gsap.min.js"></script>
    <style>
      * {{ margin: 0; padding: 0; box-sizing: border-box; }}
      html, body {{ margin: 0; width: 1920px; height: 1080px; overflow: hidden; background: #000; }}
      body {{ font-family: "Inter", "Helvetica Neue", Arial, sans-serif; }}

      .scene {{ position: absolute; inset: 0; }}
      .fill {{ position: absolute; inset: 0; background: #0b0d13; }}

      /* The caption band is reserved, so the app window letterboxes above it and
         is never covered by text. */
      .shot {{ position: absolute; left: 0; right: 0; top: 0; bottom: 112px;
              display: flex; align-items: center; justify-content: center; overflow: hidden; }}
      .shot img {{ width: 100%; height: 100%; object-fit: contain; }}

      .cap {{ position: absolute; left: 0; right: 0; bottom: 0; height: 112px;
             background: #0b0d13; border-top: 1px solid rgba(255,255,255,.09);
             display: flex; align-items: center; gap: 24px; padding: 0 54px; }}
      .tag {{ flex: none; font-size: 19px; font-weight: 700; letter-spacing: .14em;
             color: #9fd0ff; border: 1.5px solid #2f6fd0; border-radius: 999px;
             padding: 8px 18px; background: rgba(47,111,208,.18); white-space: nowrap; }}
      .q {{ font-size: 27px; font-weight: 600; color: #f4f5fb; line-height: 1.25; }}

      .card, .divider {{ position: absolute; inset: 0; background: #0b0d13;
                        display: flex; align-items: center; justify-content: center; }}
      .card .inner {{ width: 1380px; text-align: left; }}
      .kicker {{ font-size: 25px; font-weight: 700; letter-spacing: .22em; color: #5b9bff; margin-bottom: 26px; }}
      .h1 {{ font-size: 92px; font-weight: 800; color: #f5f6fc; line-height: 1.06; letter-spacing: -.015em; }}
      .h2 {{ font-size: 38px; font-weight: 500; color: #b9bdd4; margin-top: 32px; line-height: 1.4; }}
      .center {{ text-align: center; }}
      .big {{ font-size: 108px; }}
      .rows {{ margin-top: 42px; }}
      .row {{ display: flex; align-items: baseline; gap: 18px; font-size: 33px; color: #d7daea; padding: 12px 0; }}
      .row .dot {{ flex: none; width: 12px; height: 12px; border-radius: 999px; background: #2f6fd0; position: relative; top: -4px; }}
      .row b {{ color: #fff; font-weight: 700; }}
      .mono {{ font-family: "JetBrains Mono", "SF Mono", Menlo, monospace; font-size: 29px; color: #7fb1ff; }}
    </style>
  </head>
  <body>
    <div
      id="root"
      data-composition-id="main"
      data-start="0"
      data-duration="{total}"
      data-width="1920"
      data-height="1080"
      style="position: relative; width: 1920px; height: 1080px; background: #000"
    >
      <audio id="bgm" data-start="0" data-duration="{total}" data-track-index="9" data-volume="0.42" src="assets/bgm.m4a"></audio>{"".join(parts)}
    </div>

    <script>
      const tl = gsap.timeline({{ paused: true }});
      {chr(10) + "      " + (chr(10) + "      ").join(tweens)}
      window.__timelines["main"] = tl;
    </script>
  </body>
</html>
'''
io.open("index.html", "w").write(html)
print(f"{len(SCENES)} scenes, {total}s")
