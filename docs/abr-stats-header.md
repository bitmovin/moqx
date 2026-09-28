# Per-viewer connection statistics on objects (`abr_stats_header`)

When enabled on a service, the relay stamps every object it sends to a
subscriber with nine extension headers: eight describing the QUIC connection
to *that* subscriber, and one saying what interval the rates cover. A player can run bitrate adaptation from them without
measuring anything itself: the relay is the sender, so it is the one that
knows how the path is doing.

The figures describe the relay's own connection to the viewer, the last hop.
Nothing upstream of the relay is in them.

## Enabling it

```yaml
services:
  default:
    abr_stats_header:
      enabled: true
      extension_base: 16384   # 0x4000; must be even
      refresh_ms: 100         # how often a subscriber's figures are re-read
      rate_window_ms: 1000    # the window the two rates are taken over
      per_group: false        # true: only the first object of each group
```

Off by default. `extension_base` picks the extension types (see the table);
until the types are registered they are private, so keep both ends agreed.
`refresh_ms` is how often the relay re-reads the connection for one
subscriber; the figures on objects in between are repeats of the last reading.
`rate_window_ms` is the window `delivery_rate_bps` and `loss_permille` are
measured over, independent of the refresh (see Timing).

## The headers

Every field is a MoQ object extension header of an even type, so its value is
a varint. They are in the *mutable* section: they describe a hop, not the
object, and a relay downstream of this one is expected to replace them with
its own. The types are `base + 2n`:

| n | type (base 0x4000) | field | unit | what it represents |
|---|---|---|---|---|
| 0 | 0x4000 | `bw_bps` | bits/s | The congestion controller's bandwidth estimate for this connection: with BBR, the maximum delivery rate observed over the last few round trips. Long-term and stable. It cannot exceed what the relay is currently sending, so it says reliably when the link is short of the current bitrate and nothing about how much more it could take. 0 when the controller has no estimate (a non-BBR controller, or the first moments of a connection). |
| 1 | 0x4002 | `rtt_ms` | ms | The smoothed round-trip time of the connection right now, the usual exponentially weighted estimate. |
| 2 | 0x4004 | `min_rtt_ms` | ms | The lowest round-trip time seen on the connection: the propagation delay with an empty queue. Never larger than `rtt_ms`. |
| 3 | 0x4006 | `queue_delay_ms` | ms | `rtt_ms − min_rtt_ms`: how long packets are currently waiting in the bottleneck's buffer. Near 0 means the queue is empty. It rises as the bottleneck fills, *before* anything is lost, which makes it the earliest sign that the current bitrate is too much for the link. |
| 4 | 0x4008 | `loss_permille` | ‰ | Packets declared lost per thousand ack-eliciting packets sent, over the rate window. 0 on a clean link; sustained values above a few permille mean the link is dropping what the relay sends. |
| 5 | 0x400A | `cwnd_util_pct` | % | Bytes in flight as a percentage of the congestion window, 0 to 100. Near 100 means the sender is using all the window the controller allows: the network, not the media bitrate, is the limit. Well below 100 means the media is smaller than what the link would carry. |
| 6 | 0x400C | `seq` | count | A number that goes up by one every time the relay took a reading that differed from the previous one. The same reading repeated on several objects carries the same `seq`. Use it to run your logic once per reading, and to notice a connection where the figures stopped moving. Starts at 0 on the subscription. |
| 7 | 0x400E | `delivery_rate_bps` | bits/s | What the relay actually delivered to this viewer over the rate window: bytes acknowledged in the window, over the window. Short-term. It falls within one window when the link degrades, where `bw_bps` fades slowly. Like `bw_bps` it cannot exceed what was sent, so it says "less than this" reliably and "more than this" never. 0 on the first reading of a subscription, when there is nothing to measure over yet. |
| 8 | 0x4010 | `rate_window_ms` | ms | The window `delivery_rate_bps` and `loss_permille` are measured over: the relay's `rate_window_ms` setting. Constant for a subscription. Carried so a player can size its own logic to the window without being configured with it. |

All nine are always present when the header is enabled; a value the relay
does not have is 0, never a missing field.

## Reading them together

- **Going down.** `queue_delay_ms` climbing over a few readings is the early
  warning; `loss_permille` above 0 and `delivery_rate_bps` falling below the
  current rendition's bitrate confirm it. `bw_bps` follows later.
- **Going up.** No field says how much headroom there is: both bandwidth
  figures are capped by what is being sent. Probe: switch up and watch
  `queue_delay_ms`. If it stays flat, the link took it; if it climbs, come
  back down. `cwnd_util_pct` well below 100 with a flat queue is a hint that
  a probe is safe.
- **Steady state.** With a rendition well within the link,
  `delivery_rate_bps` sits at about the rendition's bitrate (it measures what
  was sent, and only that much was), `queue_delay_ms` near 0, `loss_permille`
  0.

## Timing

Three clocks are involved, and each field belongs to one of them.

**The refresh** (`refresh_ms`). The relay re-reads the connection for one
subscriber this often, and stamps the newest reading on every object until
the next one. A reading that did not move is not a new reading: `seq` stays.
So with a 300 ms refresh and 300 ms groups, `seq` moves about once per group;
with 100 ms it moves about three times per group, and an ABR that wants to
choose a rung mid-group has three chances. The reading itself is taken by the
relay's transport, which the relay asks no more often than the same interval.
The first objects of a new subscription may carry no header at all, for the
few milliseconds before the first reading lands.

**Instantaneous fields**, as of the reading: `rtt_ms`, `min_rtt_ms`,
`queue_delay_ms`, `cwnd_util_pct`, `bw_bps`.

- `rtt_ms` is QUIC's smoothed RTT (RFC 9002 §5.3): an exponentially weighted
  moving average updated on every acknowledgement that yields an RTT sample,
  with weight 1/8 on the new sample. Its memory is about eight samples, which
  on a stream acknowledged every few packets is a few tens of milliseconds to
  a few round trips: it follows a change in a fraction of a second, and one
  odd sample moves it by an eighth.
- `min_rtt_ms` is the lowest RTT sample seen over the whole life of the
  connection. It only ever falls. If the path's base delay rises (a route
  change), it stays where it was and `queue_delay_ms` then overstates the
  queue by the difference; on a connection that lasts the length of a viewing
  session that is rare.
- `queue_delay_ms` is the difference of the two above, so it moves with
  `rtt_ms`.
- `cwnd_util_pct` is bytes in flight against the congestion window at the
  instant of the reading; in-flight bytes swing within a round trip, so read
  it over several readings rather than one.
- `bw_bps` is BBR's bandwidth estimate: the maximum delivery rate observed
  over a window of about ten round trips. That window is why it is slow to
  fall and why `delivery_rate_bps` exists.

**Rates over the window** (`rate_window_ms`, default 1000): `delivery_rate_bps`
and `loss_permille`. Both are measured between the current reading and the
newest reading at least a window older, so they always cover at least the
window and slide with each reading; while a subscription is younger than the
window they cover what there is. The window itself is on every object as
`rate_window_ms`. Independent of the refresh on purpose: a
100 ms refresh with rates over 100 ms would make one lost packet read as
40‰ and one late acknowledgement swing the rate by a third. Both rates are
over the same window, so they describe the same second.

`per_group: true` stamps only the first object of each group (object id 0),
one reading per group at most, which is less than an ABR that wants to move
mid-group needs; it costs a few bytes less per object.
