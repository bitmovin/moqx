/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "relay/AbrStatsFilter.h"

#include <chrono>
#include <map>

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/test/Mocks.h>

using namespace testing;
using namespace moxygen;
using namespace openmoq::moqx;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t kBase = 0x3800;

quic::TransportInfo info(
    std::chrono::microseconds srtt,
    std::chrono::microseconds minRtt,
    uint64_t cwnd,
    uint64_t inflight,
    uint64_t bytesAcked,
    uint32_t ackEliciting,
    uint32_t lost,
    std::optional<uint64_t> bbrBps = 2'500'000
) {
  quic::TransportInfo t;
  t.srtt = srtt;
  t.maybeMinRtt = minRtt;
  t.congestionWindow = cwnd;
  t.bytesInFlight = inflight;
  t.bytesAcked = bytesAcked;
  t.totalAckElicitingPacketsSent = ackEliciting;
  t.totalPacketsMarkedLost = lost;
  if (bbrBps.has_value()) {
    quic::CongestionController::State cc;
    cc.maybeBandwidthBitsPerSec = *bbrBps;
    t.maybeCCState = cc;
  }
  return t;
}

std::map<uint64_t, uint64_t> stamped(const Extensions& ext) {
  std::map<uint64_t, uint64_t> out;
  for (const auto& e : ext.getMutableExtensions()) {
    out[e.type] = e.intValue;
  }
  return out;
}

ObjectHeader header(uint64_t group, uint64_t id) {
  return ObjectHeader{group, 0, id};
}

config::AbrStatsHeaderConfig
enabled(std::chrono::milliseconds refresh = 1h, bool perGroup = false) {
  config::AbrStatsHeaderConfig c;
  c.enabled = true;
  c.extensionBase = kBase;
  c.refresh = refresh;
  c.perGroup = perGroup;
  return c;
}

// A filter whose sampler runs inline, so the first stamp already carries a
// sample, and whose reader hands back whatever the test put in `next`.
class AbrStatsFilterTest : public Test {
protected:
  void SetUp() override {
    downstream_ = std::make_shared<NiceMock<MockTrackConsumer>>();
    ON_CALL(*downstream_, objectStream(_, _, _))
        .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*downstream_, datagram(_, _, _))
        .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    next_ = info(30ms, 20ms, 100'000, 25'000, 0, 0, 0);
  }

  // The sampler runs inline, so the first stamp already carries a sample, and
  // each reading is timed by `now_`, the way the session times its own.
  std::shared_ptr<AbrStatsFilter> make(config::AbrStatsHeaderConfig cfg) {
    filter_ = std::make_shared<AbrStatsFilter>(
        std::move(cfg),
        [](folly::Function<void()> task) {
          task();
          return true;
        },
        [this]() -> std::optional<AbrStatsFilter::Reading> {
          if (!next_) {
            return std::nullopt;
          }
          return AbrStatsFilter::Reading{now_, *next_};
        },
        downstream_
    );
    return filter_;
  }

  std::shared_ptr<NiceMock<MockTrackConsumer>> downstream_;
  std::shared_ptr<AbrStatsFilter> filter_;
  std::optional<quic::TransportInfo> next_;
  std::chrono::steady_clock::time_point now_{std::chrono::steady_clock::time_point{} + 1h};
};

} // namespace

// ---------------------------------------------------------------------------
// derive: the arithmetic, without a connection
// ---------------------------------------------------------------------------

TEST(AbrStatsDerive, FirstSampleCarriesNoRates) {
  auto s =
      AbrStatsFilter::derive(info(30ms, 20ms, 100'000, 25'000, 5'000'000, 900, 9), nullptr, 0ms, 0);
  EXPECT_EQ(s.seq, 0u);
  EXPECT_EQ(s.rttMs, 30u);
  EXPECT_EQ(s.minRttMs, 20u);
  EXPECT_EQ(s.queueDelayMs, 10u);
  EXPECT_EQ(s.cwndUtilPct, 25u);
  // The controller's estimate is a reading, not a rate: present from the start.
  EXPECT_EQ(s.bwBps, 2'500'000u);
  // No interval to take a rate over: zero, never a lifetime average.
  EXPECT_EQ(s.deliveryRateBps, 0u);
  EXPECT_EQ(s.lossPermille, 0u);
}

// srtt has the ack delay taken out, so the minimum it is compared with must
// too; otherwise queue_delay reads low.
TEST(AbrStatsDerive, MinRttIsTheOneWithoutAckDelay) {
  auto t = info(30ms, 20ms, 100'000, 0, 0, 0, 0);
  t.maybeMinRttNoAckDelay = 15ms;
  auto s = AbrStatsFilter::derive(t, nullptr, 0ms, 0);
  EXPECT_EQ(s.minRttMs, 15u);
  EXPECT_EQ(s.queueDelayMs, 15u);
}

// Losses the transport later took back are not losses, and a burst of late
// declarations that would pass 1000 is clamped to it.
TEST(AbrStatsDerive, LossExcludesSpuriousAndIsClamped) {
  auto before = info(30ms, 20ms, 1, 0, 0, 100, 0);
  auto after = info(30ms, 20ms, 1, 0, 0, 200, 8);
  after.totalPacketsSpuriouslyMarkedLost = 3;
  EXPECT_EQ(AbrStatsFilter::derive(after, &before, 1000ms, 1).lossPermille, 50u); // (8-3)/100

  auto burst = info(30ms, 20ms, 1, 0, 0, 110, 15);
  EXPECT_EQ(AbrStatsFilter::derive(burst, &before, 1000ms, 1).lossPermille, 1000u);
}

TEST(AbrStatsDerive, NoControllerEstimateIsZeroNotMissing) {
  auto s = AbrStatsFilter::derive(info(10ms, 10ms, 1, 0, 0, 0, 0, std::nullopt), nullptr, 0ms, 0);
  EXPECT_EQ(s.bwBps, 0u);
}

TEST(AbrStatsDerive, RatesAreOverTheInterval) {
  auto before = info(30ms, 20ms, 100'000, 25'000, 5'000'000, 900, 9);
  auto after = info(30ms, 20ms, 100'000, 25'000, 5'125'000, 1000, 14);
  auto s = AbrStatsFilter::derive(after, &before, 1000ms, 7);
  EXPECT_EQ(s.seq, 7u);
  EXPECT_EQ(s.deliveryRateBps, 1'000'000u); // 125 000 bytes in one second
  EXPECT_EQ(s.bwBps, 2'500'000u);           // the controller's, untouched
  EXPECT_EQ(s.lossPermille, 50u);           // 5 of 100 packets
}

TEST(AbrStatsDerive, MinRttNeverExceedsRttAndCwndUtilCapsAtHundred) {
  auto t = info(10ms, 40ms, 1'000, 5'000, 0, 0, 0);
  auto s = AbrStatsFilter::derive(t, nullptr, 0ms, 0);
  EXPECT_EQ(s.minRttMs, 10u);
  EXPECT_EQ(s.queueDelayMs, 0u);
  EXPECT_EQ(s.cwndUtilPct, 100u);
}

TEST(AbrStatsDerive, NoCwndMeansNoUtilisation) {
  auto s = AbrStatsFilter::derive(info(10ms, 10ms, 0, 5'000, 0, 0, 0), nullptr, 0ms, 0);
  EXPECT_EQ(s.cwndUtilPct, 0u);
}

// ---------------------------------------------------------------------------
// stamping
// ---------------------------------------------------------------------------

TEST_F(AbrStatsFilterTest, StampsTenVarintsOnAnObjectStream) {
  auto filter = make(enabled());
  Extensions seen;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillOnce(Invoke([&](const ObjectHeader& h, Payload, bool) {
        seen = h.extensions;
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  ASSERT_TRUE(filter->objectStream(header(5, 0), nullptr).hasValue());

  auto m = stamped(seen);
  ASSERT_EQ(m.size(), AbrStatsFilter::kFields);
  EXPECT_EQ(m[kBase + 0], 2'500'000u);  // bw_bps: the controller's estimate
  EXPECT_EQ(m[kBase + 2], 30u);         // rtt_ms
  EXPECT_EQ(m[kBase + 4], 20u);         // min_rtt_ms
  EXPECT_EQ(m[kBase + 6], 10u);         // queue_delay_ms
  EXPECT_EQ(m[kBase + 8], 0u);          // loss_permille
  EXPECT_EQ(m[kBase + 10], 25u);        // cwnd_util_pct
  EXPECT_EQ(m[kBase + 12], 1u);         // seq: the first reading replaced the zero snapshot
  EXPECT_EQ(m[kBase + 14], 0u);         // delivery_rate_bps: first sample
  EXPECT_EQ(m[kBase + 16], 1000u);      // rate_window_ms: the configured window
  EXPECT_EQ(m[kBase + 18], 3'600'000u); // refresh_ms: the configured refresh
  // Every type is even: a varint, never a byte string.
  for (const auto& [type, _] : m) {
    EXPECT_EQ(type % 2, 0u);
  }
  EXPECT_TRUE(seen.getImmutableExtensions().empty());
}

TEST_F(AbrStatsFilterTest, KeepsTheExtensionsTheObjectAlreadyHad) {
  auto filter = make(enabled());
  Extensions seen;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillOnce(Invoke([&](const ObjectHeader& h, Payload, bool) {
        seen = h.extensions;
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  ObjectHeader h = header(1, 0);
  h.extensions.insertImmutableExtension(Extension(0x02, 1500));
  h.extensions.insertMutableExtension(Extension(0x30, 1));
  ASSERT_TRUE(filter->objectStream(h, nullptr).hasValue());
  EXPECT_EQ(seen.getImmutableExtensions().size(), 1u);
  EXPECT_EQ(seen.getMutableExtensions().size(), 1u + AbrStatsFilter::kFields);
}

TEST_F(AbrStatsFilterTest, SubgroupObjectsAreStamped) {
  auto filter = make(enabled());
  auto sub = std::make_shared<NiceMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*downstream_, beginSubgroup(3, 0, _, _))
      .WillOnce(Return(
          folly::makeExpected<MoQPublishError>(std::static_pointer_cast<SubgroupConsumer>(sub))
      ));
  Extensions seen;
  EXPECT_CALL(*sub, object(4, _, _, _))
      .WillOnce(Invoke([&](uint64_t, Payload, Extensions ext, bool) {
        seen = std::move(ext);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));

  auto wrapped = filter->beginSubgroup(3, 0, 128);
  ASSERT_TRUE(wrapped.hasValue());
  ASSERT_TRUE(wrapped.value()->object(4, nullptr).hasValue());
  EXPECT_EQ(stamped(seen).size(), AbrStatsFilter::kFields);
}

// A relay behind this one may have stamped the same types; the viewer gets
// this hop's figures, once each, and anything else on the object survives.
TEST_F(AbrStatsFilterTest, ReplacesStatsAnUpstreamHopLeft) {
  auto filter = make(enabled());
  Extensions seen;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillOnce(Invoke([&](const ObjectHeader& h, Payload, bool) {
        seen = h.extensions;
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  ObjectHeader h = header(1, 0);
  h.extensions.insertMutableExtension(Extension(kBase + 2, 999));
  h.extensions.insertMutableExtension(Extension(kBase + 12, 77));
  h.extensions.insertMutableExtension(Extension(0x06, 1234));
  ASSERT_TRUE(filter->objectStream(h, nullptr).hasValue());
  std::map<uint64_t, int> count;
  for (const auto& e : seen.getMutableExtensions()) {
    count[e.type]++;
  }
  for (size_t i = 0; i < AbrStatsFilter::kFields; i++) {
    EXPECT_EQ(count[kBase + 2 * i], 1) << "type " << kBase + 2 * i;
  }
  EXPECT_EQ(count[0x06], 1);
  EXPECT_EQ(stamped(seen)[kBase + 2], 30u);
}

// With per_group, the first object of a group carries a reading taken during
// the group before it, not one a whole group old: every object refreshes.
TEST_F(AbrStatsFilterTest, PerGroupStampsACurrentReading) {
  auto filter = make(enabled(0ms, /*perGroup=*/true));
  std::vector<uint64_t> seqs;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        auto m = stamped(h.extensions);
        seqs.push_back(m.count(kBase + 12) ? m[kBase + 12] : 999);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  filter->objectStream(header(1, 1), nullptr); // mid-group join: unstamped, sampled
  next_->srtt = 31ms;
  filter->objectStream(header(2, 0), nullptr); // stamped with the reading just taken
  EXPECT_THAT(seqs, ElementsAre(999, 2));
}

TEST_F(AbrStatsFilterTest, PerGroupStampsOnlyTheFirstObject) {
  auto filter = make(enabled(1h, /*perGroup=*/true));
  std::vector<size_t> counts;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        counts.push_back(h.extensions.size());
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  filter->objectStream(header(8, 0), nullptr);
  filter->objectStream(header(8, 1), nullptr);
  filter->objectStream(header(8, 2), nullptr);
  filter->objectStream(header(9, 0), nullptr);
  EXPECT_THAT(counts, ElementsAre(AbrStatsFilter::kFields, 0, 0, AbrStatsFilter::kFields));
}

TEST_F(AbrStatsFilterTest, SequenceAdvancesOnlyWhenAnEncodedValueChanges) {
  // A refresh interval of zero re-reads on every stamp; what a player would
  // see is what decides whether the sequence moves.
  auto filter = make(enabled(0ms));
  std::vector<uint64_t> seqs;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        seqs.push_back(stamped(h.extensions)[kBase + 12]);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  filter->objectStream(header(1, 0), nullptr); // first reading replaces the zeros
  filter->objectStream(header(1, 1), nullptr); // same reading again
  next_->bytesAcked += 1000;                   // raw change, no time passed:
  filter->objectStream(header(1, 2), nullptr); // every encoded value is the same
  next_->srtt = 31ms;
  filter->objectStream(header(1, 3), nullptr); // rtt_ms moved
  filter->objectStream(header(1, 4), nullptr); // repeat
  EXPECT_THAT(seqs, ElementsAre(1, 1, 1, 2, 2));

  // A long interval repeats the snapshot, sequence included, even if the
  // connection moved underneath.
  seqs.clear();
  auto slow = make(enabled(1h));
  slow->objectStream(header(2, 0), nullptr);
  next_->srtt = 40ms;
  slow->objectStream(header(2, 1), nullptr);
  EXPECT_THAT(seqs, ElementsAre(1, 1));
}

// Before the first reading lands, a subscription's objects still carry a
// complete snapshot: all zeros but the configured settings, seq 0.
TEST_F(AbrStatsFilterTest, TheFirstObjectsCarryAZeroSnapshot) {
  std::vector<folly::Function<void()>> deferred;
  auto filter = std::make_shared<AbrStatsFilter>(
      enabled(),
      [&](folly::Function<void()> task) {
        deferred.push_back(std::move(task));
        return true;
      },
      [this]() -> std::optional<AbrStatsFilter::Reading> {
        return AbrStatsFilter::Reading{now_, *next_};
      },
      downstream_
  );
  std::vector<std::map<uint64_t, uint64_t>> seen;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        seen.push_back(stamped(h.extensions));
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  filter->objectStream(header(1, 0), nullptr);
  ASSERT_EQ(seen[0].size(), AbrStatsFilter::kFields);
  EXPECT_EQ(seen[0][kBase + 12], 0u);
  EXPECT_EQ(seen[0][kBase + 2], 0u);
  EXPECT_EQ(seen[0][kBase + 16], 1000u);
  ASSERT_EQ(deferred.size(), 1u);
  deferred[0]();
  filter->objectStream(header(1, 1), nullptr);
  EXPECT_EQ(seen[1][kBase + 12], 1u);
  EXPECT_EQ(seen[1][kBase + 2], 30u);
}

// The rates run over their own window, not over one refresh: three readings
// 300 ms apart give the same rate as one reading a second apart would, and a
// reading past the window takes its base from a window ago, not from the
// subscription's start.
TEST_F(AbrStatsFilterTest, RatesAreTakenOverTheRateWindow) {
  auto cfg = enabled(0ms);
  cfg.rateWindow = 1000ms;
  auto filter = make(cfg);
  std::vector<uint64_t> rates;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        rates.push_back(stamped(h.extensions)[kBase + 14]);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  // 12 500 bytes per 300 ms is a steady 333 333 bit/s.
  auto at = [&](std::chrono::milliseconds t, uint64_t acked) {
    now_ = std::chrono::steady_clock::time_point{} + 1h + t;
    next_->bytesAcked = acked;
    filter->objectStream(header(1, 0), nullptr);
  };
  at(0ms, 0);
  at(300ms, 12'500); // younger than the window: over what there is
  at(600ms, 25'000);
  at(1200ms, 50'000); // base is the reading a second or more ago (t=0)
  at(1500ms, 62'500); // base moves to t=300
  EXPECT_THAT(rates, ElementsAre(0, 333'333, 333'333, 333'333, 333'333));
}

TEST_F(AbrStatsFilterTest, AGoneConnectionKeepsTheLastSample) {
  auto filter = make(enabled(0ms));
  std::vector<uint64_t> rtts;
  EXPECT_CALL(*downstream_, objectStream(_, _, _))
      .WillRepeatedly(Invoke([&](const ObjectHeader& h, Payload, bool) {
        rtts.push_back(stamped(h.extensions)[kBase + 2]);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      }));
  filter->objectStream(header(1, 0), nullptr);
  next_ = std::nullopt;
  filter->objectStream(header(1, 1), nullptr);
  EXPECT_THAT(rtts, ElementsAre(30, 30));
}

// Disabled, or without a session (the PUBLISH path), nothing is stamped, but
// upstream values of these types are still stripped; everything else passes.
TEST_F(AbrStatsFilterTest, UnstampedPathsStillStripTheseTypes) {
  config::AbrStatsHeaderConfig off;
  for (auto wrapped :
       {wrapWithAbrStats(off, nullptr, downstream_),
        wrapWithAbrStats(enabled(), nullptr, downstream_)}) {
    ASSERT_NE(wrapped, downstream_);
    Extensions seen;
    EXPECT_CALL(*downstream_, objectStream(_, _, _))
        .WillOnce(Invoke([&](const ObjectHeader& h, Payload, bool) {
          seen = h.extensions;
          return folly::makeExpected<MoQPublishError>(folly::unit);
        }));
    ObjectHeader h = header(1, 0);
    h.extensions.insertMutableExtension(Extension(kBase + 2, 999));
    h.extensions.insertMutableExtension(Extension(kBase + 12, 7));
    h.extensions.insertMutableExtension(Extension(0x06, 1234));
    h.extensions.insertImmutableExtension(Extension(kBase + 4, 5));
    ASSERT_TRUE(wrapped->objectStream(h, nullptr).hasValue());
    auto m = stamped(seen);
    EXPECT_EQ(m.count(kBase + 2), 0u);
    EXPECT_EQ(m.count(kBase + 12), 0u);
    EXPECT_EQ(m[0x06], 1234u);
    // Immutable extensions are the publisher's and pass untouched.
    EXPECT_EQ(seen.getImmutableExtensions().size(), 1u);
  }
}
