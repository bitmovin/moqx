/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>

#include <folly/Executor.h>
#include <folly/Synchronized.h>
#include <moxygen/MoQFilters.h>
#include <moxygen/MoQSession.h>
#include <quic/api/TransportInfo.h>

#include "config/Config.h"

namespace openmoq::moqx {

// Stamps one subscriber's connection statistics onto the objects the relay
// writes to it, as mutable object extension headers, so a player can run ABR
// over MoQ from what the relay already knows about the path to it. There is no
// second track and nothing for the player to subscribe to: the numbers ride
// the media it is watching.
//
// One instance per SUBSCRIBE-driven downstream subscription, installed on the
// egress side of the forwarder, which is the only place the numbers are true.
// They describe *this* viewer's QUIC connection, and the same object goes to
// every viewer of the track, so a stamp taken anywhere upstream of the fan-out
// (the cache in particular, which serves late joiners the objects it retained)
// would hand one viewer another's figures. The cache never sees this filter's
// output.
//
// The statistics are the session's transport info, which only its own thread
// may read. The data path may run on another thread (a forwarder hop lands
// wherever the subscriber's executor is), so this never reads the session
// directly: when a sample is due it schedules one on the session's executor,
// and stamps from the latest sample that landed. The first objects after a
// subscription open go out unstamped, for the few milliseconds until it does.
//
// A sequence number goes up with every new sample, and a sample is new only
// when the reading changed, so a player can tell a fresh reading from a
// repeat without parsing the rest. Two bandwidth figures:
// bw_bps is the congestion controller's estimate (BBR's max delivery rate over
// the last few round trips, stable but slow to fall), delivery_rate_bps is what
// this relay measured over the last interval (bytes acked per second, which
// drops within one interval when the link degrades). Neither can exceed what
// the relay is currently sending this viewer: reliable for switching down,
// weak for switching up, which is the player's to probe.
class AbrStatsFilter : public moxygen::TrackConsumerFilter,
                       public std::enable_shared_from_this<AbrStatsFilter> {
public:
  // Reads the connection's transport info; runs on `sampleExec`. Nullopt when
  // the connection is gone, which leaves the last sample in place.
  using Reader = std::function<std::optional<quic::TransportInfo>()>;
  using Clock = std::function<std::chrono::steady_clock::time_point()>;

  AbrStatsFilter(
      config::AbrStatsHeaderConfig cfg,
      folly::Executor::KeepAlive<> sampleExec,
      Reader reader,
      std::shared_ptr<moxygen::TrackConsumer> downstream,
      Clock clock = [] { return std::chrono::steady_clock::now(); }
  );
  ~AbrStatsFilter() override = default;

  folly::Expected<std::shared_ptr<moxygen::SubgroupConsumer>, moxygen::MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      moxygen::Priority priority,
      moxygen::BeginSubgroupOptions options = {}
  ) override;

  folly::Expected<folly::Unit, moxygen::MoQPublishError> objectStream(
      const moxygen::ObjectHeader& header,
      moxygen::Payload payload,
      bool lastInGroup = false
  ) override;

  folly::Expected<folly::Unit, moxygen::MoQPublishError>
  datagram(const moxygen::ObjectHeader& header, moxygen::Payload payload, bool lastInGroup = false)
      override;

  // One reading of the connection, in the units the header carries.
  struct Sample {
    uint64_t seq{0};
    uint64_t bwBps{0};
    uint64_t deliveryRateBps{0};
    uint64_t rttMs{0};
    uint64_t minRttMs{0};
    uint64_t queueDelayMs{0};
    uint64_t lossPermille{0};
    uint64_t cwndUtilPct{0};
  };

  // Derives a sample from a reading and the one the rates are taken against
  // (the newest reading at least `rateWindow` older, or the oldest there is
  // while the subscription is younger than the window). Pure, so the
  // arithmetic is testable without a connection.
  static Sample derive(
      const quic::TransportInfo& now,
      const quic::TransportInfo* previous,
      std::chrono::milliseconds sincePrevious,
      uint64_t seq
  );

  // Appends the current sample to `extensions` if this object is due a stamp
  // (every object, or the first of each group when `perGroup`), and asks for a
  // fresh sample when the last one is older than the refresh interval.
  void stamp(moxygen::Extensions& extensions, uint64_t groupID, uint64_t objectID);

  // Extension types, in field order: bw_bps, rtt_ms, min_rtt_ms,
  // queue_delay_ms, loss_permille, cwnd_util_pct, seq, delivery_rate_bps.
  // All even: varints. The eighth came after the first seven were agreed, so
  // it takes the next slot rather than reordering them.
  static constexpr size_t kFields = 8;
  static uint64_t extensionType(uint64_t base, size_t field) {
    return base + 2 * field;
  }

  const config::AbrStatsHeaderConfig& cfg() const {
    return cfg_;
  }
  // The sample objects are being stamped with, or nullptr before the first.
  std::shared_ptr<const Sample> currentSample() const {
    return sample_.copy();
  }

private:
  void maybeRefresh();
  // Runs on sampleExec_: reads, derives, publishes the sample.
  void refreshOnExec();

  config::AbrStatsHeaderConfig cfg_;
  folly::Executor::KeepAlive<> sampleExec_;
  Reader reader_;

  // Shared between the data path and the sampler.
  folly::Synchronized<std::shared_ptr<const Sample>> sample_;
  std::atomic<bool> refreshPending_{false};
  std::atomic<int64_t> lastSampledAtNs_{0}; // steady clock, 0 = never

  // Sampler-thread state: the readings that changed, newest last, kept back
  // to one reading at least `rateWindow` old so a rate always has a base.
  struct Reading {
    std::chrono::steady_clock::time_point at;
    quic::TransportInfo info;
  };
  Clock clock_;
  std::deque<Reading> history_;
  uint64_t seq_{0};
};

// Wraps `downstream` when the header is enabled and the session is known;
// otherwise returns it untouched, so the disabled path adds nothing.
std::shared_ptr<moxygen::TrackConsumer> wrapWithAbrStats(
    const config::AbrStatsHeaderConfig& cfg,
    const std::shared_ptr<moxygen::MoQSession>& session,
    std::shared_ptr<moxygen::TrackConsumer> downstream
);

} // namespace openmoq::moqx
