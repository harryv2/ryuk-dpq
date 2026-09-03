import io, json

# how long one shot takes to dissolve into the next
OVERLAP = 0.42

# Short holds so the walkthrough reads like a recording rather than a slideshow.
# The frames that carry an idea -- priority order, slot placement -- hold longer.
SCENES = [
    ("title",   7.0, None, None, None),

    ("div",     1.9, "PART 1", "The cluster", None),
    ("shot",    3.4, "001_cluster.png", "PLACEMENT", "Four nodes. A distributed queue&rsquo;s slots spread over all of them; a single-node queue is placed whole"),
    ("shot",    3.0, "002_cluster_nodes.png", "NODES", "Each shows its container id, so a failure can be triggered by hand and watched"),

    ("div",     1.9, "PART 2", "One queue, end to end", None),
    ("shot",    2.6, "003_queues.png", "QUEUES", "Depth, in-flight, oldest age and owner node, per org"),
    ("shot",    2.2, "004_new_blank.png", "SETTINGS", "Visibility timeout, retry limit, TTL, starvation threshold and reserve &mdash; and one placement choice"),
    ("shot",    1.6, "005_new_named.png", "NAME", "Validated in the controller, before anything is stored"),
    ("shot",    2.0, "006_created.png", "CREATED", "Placed on a node and empty. Nothing ready, nothing in flight"),

    ("div",     1.9, "PART 3", "Send four", None),
    ("shot",    2.0, "007_sent_one.png", "SEND", "First message at HIGH. What was queued appears beside the form"),
    ("shot",    3.6, "008_sent_four.png", "FOUR IN", "Two HIGH, one MEDIUM, one LOW. Ready reads 4"),

    ("div",     1.9, "PART 4", "Priority, acknowledge, retry", None),
    ("shot",    2.0, "009_poll_ready.png", "ONE AT A TIME", "Taking a single message per poll, so the order the queue hands them out is visible"),
    ("shot",    3.2, "010_poll_high1.png", "HIGH FIRST", "Ready 4 &rarr; 3, in flight 1. The first HIGH comes out, attempt 1"),
    ("shot",    3.0, "011_ack_high1.png", "ACK", "In flight back to 0, ready stays at 3 &mdash; acknowledged means gone, not returned"),
    ("shot",    2.6, "012_poll_high2.png", "SECOND HIGH", "Ready 3 &rarr; 2. Both HIGH messages come out before anything else"),
    ("shot",    3.4, "013_nack_high2.png", "NACK", "Ready climbs back to 3 &mdash; a nack returns the message instead of consuming it"),
    ("shot",    3.6, "014_poll_high2_again.png", "REDELIVERED", "The same message, now attempt 2. Enough failures and it goes to the dead-letter queue"),
    ("shot",    2.6, "015_poll_medium.png", "THEN MEDIUM", "Only once both HIGH messages are done does MEDIUM get its turn"),
    ("shot",    2.6, "016_poll_low.png", "THEN LOW", "LOW last &mdash; strict priority, FIFO inside each band"),
    ("shot",    2.6, "017_drained.png", "DRAINED", "Four in, four out, one of them twice because it was nacked"),

    ("div",     1.9, "PART 5", "Scheduling for later", None),
    ("shot",    2.8, "064_delay_form.png", "DELIVER AFTER", "A message can carry a delivery time &mdash; twenty-five seconds here, but it is the same mechanism as a retry backoff"),
    ("shot",    3.2, "065_delay_sent.png", "HELD BACK", "Two sent, one ready and one delayed. The delayed one is counted separately, not as depth"),
    ("shot",    3.0, "066_delay_held_back.png", "ONLY THE OTHER", "A poll takes the message that is due and leaves the scheduled one alone"),
    ("shot",    3.0, "067_delay_only_delayed.png", "STILL WAITING", "The immediate one acknowledged. Ready is 0, delayed is still 1"),
    ("shot",    3.2, "068_delay_nothing_yet.png", "NOT YET", "Polling again takes nothing &mdash; no consumer can reach it before its time"),
    ("shot",    3.4, "069_delay_released.png", "RELEASED", "Its moment arrives and the sweeper moves it across: delayed 1 &rarr; 0, ready 0 &rarr; 1"),
    ("shot",    3.4, "070_delay_arrived.png", "DELIVERED", "Now it comes out, on its first attempt, having waited exactly as long as it was told to"),

    ("div",     2.2, "PART 6", "Metrics, live", None),
    ("shot",    3.0, "018_metrics_quiet.png", "AT REST", "Prometheus scrapes the gateway, so the history survives a reload and reaches back further than this tab has been open"),
]

# producers outrunning consumers -- a time-lapse, so these hold barely a beat
GROWING = [
    ("019_metrics_growing_00.png", "BACKLOG BUILDING", "Producers at five a second, consumers taking one"),
    ("020_metrics_growing_01.png", None, None),
    ("021_metrics_growing_02.png", None, None),
    ("022_metrics_growing_03.png", "WRITE ABOVE ACK", "The two throughput charts separate: arrivals climb, acknowledgements do not"),
    ("023_metrics_growing_04.png", None, None),
    ("024_metrics_growing_05.png", None, None),
    ("025_metrics_growing_06.png", "DEPTH CLIMBING", "Every band rising together, and the oldest message getting older"),
    ("026_metrics_growing_07.png", None, None),
    ("027_metrics_growing_08.png", None, None),
    ("028_metrics_growing_09.png", None, None),
    ("029_metrics_growing_10.png", "SIX HUNDRED DEEP", "Thirteen a second in against two and a half out. This is what falling behind looks like"),
]
DRAINING = [
    ("030_metrics_draining_00.png", "CONSUMERS CATCH UP", "The ratio flips &mdash; one a second in, six out"),
    ("031_metrics_draining_01.png", None, None),
    ("032_metrics_draining_02.png", None, None),
    ("033_metrics_draining_03.png", "ACK ABOVE WRITE", "Sixteen a second acknowledged against two and a half arriving"),
    ("034_metrics_draining_04.png", None, None),
    ("035_metrics_draining_05.png", None, None),
    ("036_metrics_draining_06.png", "HIGH GOES FIRST", "The red band reaches zero before the others &mdash; priority holds under load"),
    ("037_metrics_draining_07.png", None, None),
    ("038_metrics_draining_08.png", "DRAINED", "Back to a hundred, and still falling"),
]

for f, tag, cap in GROWING + DRAINING:
    SCENES.append(("shot", 1.9 if tag else 0.9, f, tag or "", cap or ""))

SCENES += [
    ("shot",    3.0, "039_metrics_total.png", "TOTAL", "One line for the sum across every priority, when the overall depth is what matters"),
    ("shot",    2.8, "040_metrics_filtered.png", "FILTER", "Hiding a band rescales the axis, so a small series is not flattened by a large one"),
    ("shot",    2.8, "041_metrics_window.png", "WINDOW", "Five minutes, an hour, six, a day &mdash; served from stored history, not from this tab"),

    ("div",     1.9, "PART 7", "Distributed queues", None),
    ("shot",    2.8, "042_events_queue.png", "SPREAD", "Slots placed independently, so counts are a point-in-time sum rather than an exact figure"),
    ("shot",    2.8, "043_events_metrics.png", "SUMMED", "Every machine holding a slot is asked, and the answers added up"),
    ("shot",    3.2, "044_placement.png", "SIXTY-FOUR SLOTS", "Spread over four nodes. The bar is how much of the queue each one holds"),

    ("div",     1.9, "PART 8", "Adding machines", None),
    ("shot",    2.2, "045_scaling_00.png", "SCALE TO SIX", "docker compose up --scale node=6. Nodes register themselves; nothing is told about them"),
    ("shot",    1.0, "046_scaling_01.png", "", ""),
    ("shot",    1.0, "047_scaling_02.png", "", ""),
    ("shot",    1.9, "048_scaling_03.png", "REBALANCING", "Placement is a pure function of the member list, so every gateway agrees on what should move"),
    ("shot",    1.0, "049_scaling_04.png", "", ""),
    ("shot",    1.0, "050_scaling_05.png", "", ""),
    ("shot",    1.0, "051_scaling_06.png", "", ""),
    ("shot",    3.4, "052_scaling_07.png", "SIX NODES", "The slots moved onto the new machines. Freeze, ship, absorb &mdash; no message lost, no order broken"),

    ("div",     1.9, "PART 9", "Losing a machine", None),
    ("shot",    2.4, "053_node_down_00.png", "DOCKER STOP", "One container killed outright"),
    ("shot",    1.2, "054_node_down_01.png", "", ""),
    ("shot",    1.2, "055_node_down_02.png", "", ""),
    ("shot",    4.0, "056_node_down_03.png", "ONLY ITS SLOTS", "Five nodes. The gap in the bar is the slots that went with it &mdash; the rest keep serving"),
    ("shot",    3.4, "057_events_degraded.png", "HONEST COUNTS", "The stats say how many slots are unreachable rather than reporting a smaller queue"),
    ("shot",    3.4, "058_node_back.png", "AND BACK", "Restarted, it replays its log, re-registers, and its slots are whole again"),

    ("div",     1.9, "PART 10", "Tenancy and theme", None),
    ("shot",    2.2, "059_acme.png", "ACME", "Everything so far belongs to one org"),
    ("shot",    3.0, "060_globex.png", "GLOBEX", "The org comes from the credential, never the URL &mdash; another tenant sees none of it"),
    ("shot",    1.8, "061_dark_queues.png", "DARK", "Light and dark, remembered per browser"),
    ("shot",    2.4, "062_dark_metrics.png", "SAME CHARTS", "Nothing is redrawn for a theme; the palette is read from tokens"),
    ("shot",    2.2, "063_dark_cluster.png", "SAME CLUSTER", ""),

    ("outro",   10.0, None, None, None),
]

parts, tweens, t = [], [], 0.0
i = 0
for kind, dur, a, b, c in SCENES:
    i += 1
    sid = f"s{i:02d}"
    # Every scene runs a little past its slot and the next one fades in over the
    # top, so one shot dissolves into the next. Without the overlap each clip
    # ends, its opaque fill goes with it, and the frame flashes dark before the
    # next image arrives -- which is the flicker between screenshots.
    lap = round(min(OVERLAP, dur * 0.4), 2)
    start = round(max(0.0, t - (lap if i > 1 else 0.0)), 2)
    dur = round(dur + (lap if i > 1 else 0.0), 2)
    t += dur - (lap if i > 1 else 0.0)
    fade = lap if i > 1 else 0.0

    if fade:
        tweens.append(
            f'tl.fromTo("#{sid}", {{ autoAlpha: 0 }}, {{ autoAlpha: 1, duration: {fade}, ease: "sine.inOut" }}, {start});')

    if kind == "title":
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0" style="z-index: {i}">
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
            tweens.append(f'tl.fromTo("#{sid}-{el}", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.6, ease: "power2.out" }}, {round(start+fade+off,2)});')

    elif kind == "div":
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0" style="z-index: {i}">
        <div class="fill"></div>
        <div class="divider">
          <div>
            <div class="kicker center" id="{sid}-k">{a}</div>
            <div class="h1 center big" id="{sid}-h">{b}</div>
          </div>
        </div>
      </div>''')
        tweens.append(f'tl.fromTo("#{sid}-k", {{ autoAlpha: 0, y: 18 }}, {{ autoAlpha: 1, y: 0, duration: 0.4, ease: "power2.out" }}, {round(start+fade+0.12,2)});')
        tweens.append(f'tl.fromTo("#{sid}-h", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.5, ease: "power2.out" }}, {round(start+fade+0.28,2)});')

    elif kind == "shot":
        # A frame with no caption is one step of a time-lapse: the band from the
        # frame before it stays up, so the run reads as one continuous shot.
        cap = ""
        if b:
            cap = f'\n        <div class="cap" id="{sid}-c"><span class="tag">{b}</span><span class="q">{c}</span></div>'
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0" style="z-index: {i}">
        <div class="fill"></div>
        <div class="shot" data-layout-allow-overflow><img id="{sid}-i" src="assets/{a}" alt="" /></div>{cap}
      </div>''')
        if b:
            tweens.append(f'tl.fromTo("#{sid}-c", {{ autoAlpha: 0, y: 22 }}, {{ autoAlpha: 1, y: 0, duration: 0.42, ease: "power2.out" }}, {round(start+fade+0.1,2)});')
        # a touch of scale so a cut between two near-identical frames still reads
        tweens.append(f'tl.fromTo("#{sid}-i", {{ scale: 1.006 }}, {{ scale: 1, duration: 0.7, ease: "power2.out" }}, {start});')

    else:  # outro
        parts.append(f'''
      <div class="clip scene" id="{sid}" data-start="{start}" data-duration="{dur}" data-track-index="0" style="z-index: {i}">
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
            tweens.append(f'tl.fromTo("#{sid}-{el}", {{ autoAlpha: 0, y: 26 }}, {{ autoAlpha: 1, y: 0, duration: 0.65, ease: "power2.out" }}, {round(start+fade+off,2)});')

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
