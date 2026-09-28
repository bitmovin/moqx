/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "relay/AbrStatsFilter.h"

#include <algorithm>

namespace openmoq::moqx {

namespace {

// The subgroup half: every object of one subgroup passes through here, and
// the extensions it carries are what the subscriber's decoder sees.
class AbrStatsSubgroupFilter : public moxygen::SubgroupConsumerFilter {
public:
  AbrStatsSubgroupFilter(
      std::shared_ptr<AbrStatsFilter> parent,
      uint64_t groupID,
      std::shared_ptr<moxygen::SubgroupConsumer> downstream
  )
      : moxygen::SubgroupConsumerFilter(std::move(downstream)), parent_(std::move(parent)),
        groupID_(groupID) {}

  folly::Expected<folly::Unit, moxygen::MoQPublishError> object(
      uint64_t objectID,
      moxygen::Payload payload,
      moxygen::Extensions extensions = moxygen::noExtensions(),
      bool finSubgroup = false
  ) override {
    parent_->stamp(extensions, groupID_, objectID);
    return downstream_->object(objectID, std::move(payload), std::move(extensions), finSubgroup);
  }

  folly::Expected<folly::Unit, moxygen::MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      moxygen::Payload initialPayload,
      moxygen::Extensions extensions = moxygen::noExtensions()
  ) override {
    parent_->stamp(extensions, groupID_, objectID);
    return downstream_
        ->beginObject(objectID, length, std::move(initialPayload), std::move(extensions));
  }

private:
  std::shared_ptr<AbrStatsFilter> parent_;
  uint64_t groupID_;
};

uint64_t millis(std::chrono::microseconds us) {
  return static_cast<uint64_t>(std::max<int64_t>(0, us.count()) / 1000);
}

// The fields a sample is derived from. Two readings that agree on all of
// them are the same reading, whatever else in the struct moved.
bool sameReading(const quic::TransportInfo& a, const quic::TransportInfo& b) {
  const auto bw = [](const quic::TransportInfo& t) -> std::optional<uint64_t> {
    if (t.maybeCCState.has_value() && t.maybeCCState.value().maybeBandwidthBitsPerSec.has_value()) {
      return t.maybeCCState.value().maybeBandwidthBitsPerSec.value();
    }
    return std::nullopt;
  };
  const auto minRtt = [](const quic::TransportInfo& t) -> std::optional<int64_t> {
    if (t.maybeMinRttNoAckDelay.has_value()) {
      return t.maybeMinRttNoAckDelay.value().count();
    }
    if (t.maybeMinRtt.has_value()) {
      return t.maybeMinRtt.value().count();
    }
    return std::nullopt;
  };
  return a.srtt == b.srtt && minRtt(a) == minRtt(b) && a.congestionWindow == b.congestionWindow &&
         a.bytesInFlight == b.bytesInFlight && a.bytesAcked == b.bytesAcked &&
         a.totalAckElicitingPacketsSent == b.totalAckElicitingPacketsSent &&
         a.totalPacketsMarkedLost == b.totalPacketsMarkedLost && bw(a) == bw(b);
}

// Whether two samples carry the same values on the wire, seq aside.
bool sameValues(const AbrStatsFilter::Sample& a, const AbrStatsFilter::Sample& b) {
  return a.bwBps == b.bwBps && a.deliveryRateBps == b.deliveryRateBps && a.rttMs == b.rttMs &&
         a.minRttMs == b.minRttMs && a.queueDelayMs == b.queueDelayMs &&
         a.lossPermille == b.lossPermille && a.cwndUtilPct == b.cwndUtilPct;
}

int64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch()
  )
      .count();
}

} // namespace

AbrStatsFilter::AbrStatsFilter(
    config::AbrStatsHeaderConfig cfg,
    Scheduler schedule,
    Reader reader,
    std::shared_ptr<moxygen::TrackConsumer> downstream
)
    : moxygen::TrackConsumerFilter(std::move(downstream)), cfg_(std::move(cfg)),
      schedule_(std::move(schedule)), reader_(std::move(reader)),
      // A complete all-zero snapshot with seq 0 from the start, so the first
      // object of a subscription is stamped too; the first reading replaces it.
      sample_(std::make_shared<const Sample>()) {}

folly::Expected<std::shared_ptr<moxygen::SubgroupConsumer>, moxygen::MoQPublishError>
AbrStatsFilter::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    moxygen::Priority priority,
    moxygen::BeginSubgroupOptions options
) {
  auto res = downstream_->beginSubgroup(groupID, subgroupID, priority, options);
  if (res.hasError()) {
    return res;
  }
  return std::static_pointer_cast<moxygen::SubgroupConsumer>(
      std::make_shared<AbrStatsSubgroupFilter>(shared_from_this(), groupID, std::move(res.value()))
  );
}

folly::Expected<folly::Unit, moxygen::MoQPublishError> AbrStatsFilter::objectStream(
    const moxygen::ObjectHeader& header,
    moxygen::Payload payload,
    bool lastInGroup
) {
  moxygen::ObjectHeader stamped = header;
  stamp(stamped.extensions, stamped.group, stamped.id);
  return downstream_->objectStream(stamped, std::move(payload), lastInGroup);
}

folly::Expected<folly::Unit, moxygen::MoQPublishError> AbrStatsFilter::datagram(
    const moxygen::ObjectHeader& header,
    moxygen::Payload payload,
    bool lastInGroup
) {
  moxygen::ObjectHeader stamped = header;
  stamp(stamped.extensions, stamped.group, stamped.id);
  return downstream_->datagram(stamped, std::move(payload), lastInGroup);
}

AbrStatsFilter::Sample AbrStatsFilter::derive(
    const quic::TransportInfo& now,
    const quic::TransportInfo* previous,
    std::chrono::milliseconds sincePrevious,
    uint64_t seq
) {
  Sample s;
  s.seq = seq;
  s.rttMs = millis(now.srtt);
  // The minimum without ack delay, because srtt has the peer's ack delay
  // taken out and the two are subtracted: the minimum with ack delay would
  // make queue_delay read low. mvfst keeps both as optionals; before the
  // first reading the smoothed RTT is the best floor there is.
  uint64_t minRttMs = s.rttMs;
  if (now.maybeMinRttNoAckDelay.has_value()) {
    minRttMs = millis(now.maybeMinRttNoAckDelay.value());
  } else if (now.maybeMinRtt.has_value()) {
    minRttMs = millis(now.maybeMinRtt.value());
  }
  s.minRttMs = std::min(minRttMs, s.rttMs);
  s.queueDelayMs = s.rttMs - s.minRttMs;
  s.cwndUtilPct = now.congestionWindow > 0
                      ? std::min<uint64_t>(100, now.bytesInFlight * 100 / now.congestionWindow)
                      : 0;
  // The controller's own estimate; BBR fills it, Cubic and NewReno leave it
  // empty, and an empty one is 0 on the wire rather than a missing field, so a
  // stamped object always carries all ten.
  if (now.maybeCCState.has_value() &&
      now.maybeCCState.value().maybeBandwidthBitsPerSec.has_value()) {
    s.bwBps = now.maybeCCState.value().maybeBandwidthBitsPerSec.value();
  }
  if (previous != nullptr && sincePrevious.count() > 0) {
    // Delivery rate and loss over the interval, not since the connection
    // opened: a viewer whose first minute was rough should not carry it for
    // the rest of the session.
    const uint64_t acked =
        now.bytesAcked >= previous->bytesAcked ? now.bytesAcked - previous->bytesAcked : 0;
    s.deliveryRateBps = acked * 8 * 1000 / static_cast<uint64_t>(sincePrevious.count());
    const uint64_t sent =
        now.totalAckElicitingPacketsSent >= previous->totalAckElicitingPacketsSent
            ? now.totalAckElicitingPacketsSent - previous->totalAckElicitingPacketsSent
            : 0;
    const uint64_t lost = now.totalPacketsMarkedLost >= previous->totalPacketsMarkedLost
                              ? now.totalPacketsMarkedLost - previous->totalPacketsMarkedLost
                              : 0;
    // Declarations and sends counted by when they happened, not by packet
    // cohort, so a burst of late declarations can exceed 1000. Reported as
    // measured rather than clamped.
    s.lossPermille = sent > 0 ? lost * 1000 / sent : 0;
  }
  // A first reading has nothing to take a rate over: zero rather than a
  // lifetime average, so a player never mistakes a connection-long figure for
  // a current one. The next sample carries real rates.
  return s;
}

void AbrStatsFilter::maybeRefresh() {
  const int64_t now = nowNs();
  const int64_t last = lastSampledAtNs_.load(std::memory_order_relaxed);
  const int64_t refreshNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(cfg_.refresh).count();
  if (last != 0 && now - last < refreshNs) {
    return;
  }
  bool expected = false;
  if (!refreshPending_.compare_exchange_strong(expected, true)) {
    return;
  }
  const bool scheduled = schedule_([weak = weak_from_this()] {
    if (auto self = weak.lock()) {
      self->refreshOnExec();
    }
  });
  if (!scheduled) {
    // The session is gone; the last sample stays, and the next object may ask
    // again rather than finding a refresh forever pending.
    refreshPending_.store(false, std::memory_order_release);
  }
}

void AbrStatsFilter::refreshOnExec() {
  if (auto reading = reader_()) {
    // Rates are taken between the times the transport was read. The session
    // caches one reading for every caller (this subscription's siblings on
    // the same session among them), so the time this filter asked says
    // nothing about the age of what it got.
    const auto now = reading->at;
    const quic::TransportInfo* info = &reading->info;
    // A reading that did not move is not a new sample: the sequence number
    // stays. The session hands out one cached reading per its own interval,
    // so asking more often than that would otherwise count repeats as changes.
    if (history_.empty() || !sameReading(*info, history_.back().info)) {
      Sample next;
      if (history_.empty()) {
        next = derive(*info, nullptr, std::chrono::milliseconds(0), seq_);
      } else {
        // The rates run against the newest reading at least a window old,
        // so they are over the window rather than over one refresh, or
        // against the oldest reading there is while the subscription is
        // younger than the window.
        const Reading* base = &history_.front();
        for (auto it = history_.rbegin(); it != history_.rend(); ++it) {
          if (now - it->at >= cfg_.rateWindow) {
            base = &*it;
            break;
          }
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - base->at);
        next = derive(*info, &base->info, elapsed, seq_);
      }
      history_.push_back(Reading{now, *info});
      // Keep exactly one reading that is at least a window old, drop the rest.
      while (history_.size() > 1 && now - history_[1].at >= cfg_.rateWindow) {
        history_.pop_front();
      }
      // The sequence moves when a value a player sees changes, compared after
      // integer conversion, so a raw change that rounds away is not news.
      auto published = sample_.copy();
      if (!sameValues(next, *published)) {
        next.seq = ++seq_;
        *sample_.wlock() = std::make_shared<const Sample>(next);
      }
    }
  }
  lastSampledAtNs_.store(nowNs(), std::memory_order_relaxed);
  refreshPending_.store(false, std::memory_order_release);
}

void AbrStatsFilter::stamp(moxygen::Extensions& extensions, uint64_t groupID, uint64_t objectID) {
  (void)groupID;
  // Refresh on every object, stamped or not, so the first object of the next
  // group carries a reading taken during this one rather than one group old.
  maybeRefresh();
  if (cfg_.perGroup && objectID != 0) {
    return;
  }
  const auto s = sample_.copy();
  // Mutable, because they describe a hop, not the object: a relay behind this
  // one (or a publisher) may have stamped the same types, and this viewer
  // wants this hop's figures, not the one upstream of it.
  const uint64_t base = cfg_.extensionBase;
  auto& mutableExts = extensions.getMutableExtensions();
  mutableExts.erase(
      std::remove_if(
          mutableExts.begin(),
          mutableExts.end(),
          [base](const moxygen::Extension& ext) {
            return ext.type >= base && ext.type <= extensionType(base, kFields - 1) &&
                   (ext.type - base) % 2 == 0;
          }
      ),
      mutableExts.end()
  );
  const uint64_t values[kFields] = {
      s->bwBps,
      s->rttMs,
      s->minRttMs,
      s->queueDelayMs,
      s->lossPermille,
      s->cwndUtilPct,
      s->seq,
      s->deliveryRateBps,
      static_cast<uint64_t>(cfg_.rateWindow.count()),
      static_cast<uint64_t>(cfg_.refresh.count()),
  };
  for (size_t i = 0; i < kFields; i++) {
    extensions.insertMutableExtension(moxygen::Extension(extensionType(base, i), values[i]));
  }
}

std::shared_ptr<moxygen::TrackConsumer> wrapWithAbrStats(
    const config::AbrStatsHeaderConfig& cfg,
    const std::shared_ptr<moxygen::MoQSession>& session,
    std::shared_ptr<moxygen::TrackConsumer> downstream
) {
  if (!cfg.enabled || !session || !downstream) {
    return downstream;
  }
  if (session->getExecutor() == nullptr) {
    return downstream;
  }
  // Both hold the session weakly: a subscription's consumer can outlive the
  // session by a little, and a gone session simply stops the samples. Neither
  // holds its executor, whose thread must be free to stop with the relay.
  std::weak_ptr<moxygen::MoQSession> weak = session;
  AbrStatsFilter::Scheduler schedule = [weak](folly::Function<void()> task) {
    auto s = weak.lock();
    if (!s || s->getExecutor() == nullptr) {
      return false;
    }
    s->getExecutor()->add(std::move(task));
    return true;
  };
  // Runs on the session's executor, the only thread that may touch the
  // session's transport-info cache, so the interval is set here and not on
  // the thread that built the subscription. It only ever lowers the session's.
  const auto refresh = cfg.refresh;
  AbrStatsFilter::Reader reader = [weak, refresh]() -> std::optional<AbrStatsFilter::Reading> {
    auto s = weak.lock();
    if (!s) {
      return std::nullopt;
    }
    s->setTransportInfoCacheDuration(refresh);
    auto timed = s->getTimedTransportInfo();
    return AbrStatsFilter::Reading{timed.takenAt, std::move(timed.info)};
  };
  return std::make_shared<AbrStatsFilter>(
      cfg,
      std::move(schedule),
      std::move(reader),
      std::move(downstream)
  );
}

} // namespace openmoq::moqx
