#include "kqp_compute_scheduler.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

Y_UNIT_TEST_SUITE(TComputeScheduler) {

    TVector<TDuration> RunSimulation(TComputeScheduler& scheduler, TArrayRef<TSchedulerEntityHandle> handles, TMonotonic start, TDuration tick, TDuration length, TDuration batch) {
        TVector<TDuration> results(handles.size());
        for (TDuration t = TDuration::Zero(); t < length; t += tick) {
            auto now = start + t;
            for (size_t i = 0; i < handles.size(); ++i) {
                auto& handle = handles[i];
                if (!handle.Delay(now)) {
                    handle.TrackTime(batch, now);
                    results[i] += batch;
                }
            }
            scheduler.AdvanceTime(now);
        }
        return results;
    }


    Y_UNIT_TEST(TTotalLimits) {
        TComputeScheduler scheduler;
        scheduler.UpdateGroupShare("first", 0.4, TMonotonic::Zero(), std::nullopt);
        scheduler.UpdateGroupShare("second", 0.4, TMonotonic::Zero(), std::nullopt);
        scheduler.SetMaxDeviation(TDuration::MilliSeconds(10));
        scheduler.SetCapacity(2);
        TVector<TSchedulerEntityHandle> handles;
        handles.push_back(scheduler.Enroll("first", 1, TMonotonic::Zero()));
        handles.push_back(scheduler.Enroll("second", 1, TMonotonic::Zero()));
        auto times = RunSimulation(scheduler,
            handles,
            TMonotonic::Zero() + TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(1),
            TDuration::Seconds(2),
            TDuration::MilliSeconds(10));
        for (auto& time : times) {
            Cerr << time.MilliSeconds() << " " << (TDuration::Seconds(2) * 0.8).MilliSeconds() << Endl;
            UNIT_ASSERT_LE(time, TDuration::Seconds(2) * 0.8 + TDuration::MilliSeconds(10));
            UNIT_ASSERT_GE(time, TDuration::Seconds(2) * 0.8 - TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(QueryLimits) {
    }

    Y_UNIT_TEST(ResourceWeight) {
    }
}
