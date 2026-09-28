# Per-viewer connection statistics on objects (`abr_stats_header`)

When enabled on a service, the relay stamps every object it sends to a
subscriber with ten extension headers: eight describing the QUIC connection
from this relay to *that* subscriber (all traffic on it, every subscription
included; nothing about the publisher's side), and two carrying the relay's
settings. The default range, 0x3800-0x3812, is application-specific:
draft-18 reserves 0x4000-0x7FFF for mandatory track properties.

A player can run bitrate adaptation from them without
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
      extension_base: 14336   # 0x3800; must be even
      refresh_ms: 100         # how often a subscriber's figures are re-read
      rate_window_ms: 1000    # the window the two rates are taken over
      per_group: false        # true: only the first object of each group
```

Off by default. Defaults when enabled: `extension_base` 14336 (0x3800),
`refresh_ms` 1000, `rate_window_ms` 1000, `per_group` false; the example above
is a setting for a player that wants several readings per group.
`extension_base` picks the extension types (see the table);
until the types are registered they are private, so keep both ends agreed.
`refresh_ms` is how often the relay re-reads the connection for one
subscriber; the figures on objects in between are repeats of the last reading.
`rate_window_ms` is the window `delivery_rate_bps` and `loss_permille` are
measured over, independent of the refresh (see Timing).

## The headers

Every field is a MoQ object extension header of an even type, so its value is
a varint. They are in the *mutable* section: they describe a hop, not the
object. The relay removes any mutable values of these types that arrive from
upstream (a publisher, or a relay behind it) from every object it forwards,
stamped or not, and even with the header disabled; a viewer only ever sees
this relay's figures for its own connection, once each. Immutable extensions
are the publisher's and pass untouched. The types are `base + 2n`:

| n | type (base 0x3800) | field | unit | what it represents |
|---|---|---|---|---|
| 0 | 0x3800 | `bw_bps` | bits/s | The congestion controller's bandwidth estimate for this connection: with BBR, the maximum delivery rate observed over the last few round trips. Long-term and stable. It cannot exceed what the relay is currently sending, so it says reliably when the link is short of the current bitrate and nothing about how much more it could take. 0 when the controller has no estimate (a non-BBR controller, or the first moments of a connection). |
| 1 | 0x3802 | `rtt_ms` | ms | The smoothed round-trip time of the connection right now, the usual exponentially weighted estimate. |
| 2 | 0x3804 | `min_rtt_ms` | ms | The lowest RTT seen on the connection since it opened, without ack delay (srtt has the peer's ack delay removed, so the two compare like for like): the propagation delay with an empty queue. No rolling expiry; only ever falls. Never larger than `rtt_ms`. |
| 3 | 0x3806 | `queue_delay_ms` | ms | `rtt_ms − min_rtt_ms`, from the transmitted integer values: excess round-trip delay, most of it waiting in the bottleneck's buffer. Near 0 means the queue is empty; it rises as a buffer fills, before anything is lost, which makes it the earliest sign that the bitrate is too much for the link. It does not say which hop the queue is at. |
| 4 | 0x3808 | `loss_permille` | ‰ | Packets declared lost in the rate window, minus those the transport later took back (acknowledged after all), per thousand ack-eliciting packets sent in it: `min(1000, floor(1000 × lost / sent))`. Declarations and sends are counted by when they happen, so a burst of late declarations can briefly outrun sends; clamped at 1000, which already says the link is as bad as it gets. 0 when nothing was sent. |
| 5 | 0x380A | `cwnd_util_pct` | % | `min(100, floor(100 × bytes_in_flight / cwnd))` at the reading, 0 without a window. Window use, not a share of bandwidth: under BBR the window is about twice the bandwidth-delay product, so a full link reads about **50**, not 100. `queue_delay_ms` together with `delivery_rate_bps` close to `bw_bps` is the better "link is full" signal. |
| 6 | 0x380C | `seq` | count | Per subscription. 0 on the initial zero snapshot; goes up by one whenever any other field's *encoded* value changes (compared after integer conversion, so a change that rounds away is not news). Every object carries the newest snapshot, so the same `seq` repeats until a value moves, and a player that misses objects sees gaps. Unrelated across subscriptions. |
| 7 | 0x380E | `delivery_rate_bps` | bits/s | Bytes acknowledged over the rate window (mvfst `bytesAcked`: whole QUIC packets, headers included, so transport throughput rather than media goodput), times 8, over the window. Short-term: falls within one window when the link degrades, where `bw_bps` fades slowly. Capped by what was offered, so it says "less than this" reliably and "more than this" never. 0 on the first reading; until a full window exists, measured over the time available. |
| 8 | 0x3810 | `rate_window_ms` | ms | The window `delivery_rate_bps` and `loss_permille` are measured over: the relay's `rate_window_ms` setting. Constant for a subscription. Carried so a player can size its own logic to the window without being configured with it. |
| 9 | 0x3812 | `refresh_ms` | ms | How often the relay takes a reading for this subscription: the relay's `refresh_ms` setting. Constant for a subscription. Separate from the rate window. |

A stamped object carries all ten; a value the relay does not have is 0,
never a missing field. Every object of a SUBSCRIBE-driven subscription is
stamped (only the first of each group with `per_group`), from the very first:
until the first reading lands, objects carry an all-zero snapshot with seq 0.
Stamping every object rather than only on change means a lost object never
loses a reading. Objects delivered because the relay published a
track to a subscriber (PUBLISH, not SUBSCRIBE) are never stamped.

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
mid-group needs; it costs a few bytes less per object. Readings are still
taken on every object, so the stamp on a group's first object is current, and
a viewer that joins mid-group gets its first stamp at the next group.

`rate_window_ms` shorter than `refresh_ms` is accepted with a warning: a rate
is taken between two readings, so it then covers the refresh interval.
