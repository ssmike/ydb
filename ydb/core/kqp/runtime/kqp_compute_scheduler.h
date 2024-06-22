#pragma once

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr {
namespace NKqp {

class TSchedulerEntity;
class TSchedulerEntityHandle {
private:
    std::unique_ptr<TSchedulerEntity> Ptr;

public:
    TSchedulerEntityHandle(TSchedulerEntity*);

    TSchedulerEntityHandle();
    TSchedulerEntityHandle(TSchedulerEntityHandle&&); 

    TSchedulerEntityHandle& operator = (TSchedulerEntityHandle&&);

    operator bool () {
        return Ptr.get() != nullptr;
    }

    TSchedulerEntity& operator*() {
        return *Ptr;
    }

    double VRuntime();

    void TrackTime(TDuration time);
    TMaybe<TDuration> CalcDelay(TMonotonic now);

    TMaybe<TDuration> Lag(TMonotonic now);
    //double LagVTime(TMonotonic now);
    double GroupNow(TMonotonic now);

    double EstimateWeight(TMonotonic now, TDuration minTime);

    void Clear();

    ~TSchedulerEntityHandle();
};

class TComputeScheduler {
public:
    struct TDistributionRule {
        double Share;
        TString Name;
        TVector<TDistributionRule> SubRules;

        bool empty() {
            return SubRules.empty() && Name.empty();
        }
    };

public:
    TComputeScheduler();
    ~TComputeScheduler();

    void ReportCounters(TIntrusivePtr<TKqpCounters>);

    void SetPriorities(TDistributionRule rootRule, double cores, TMonotonic now);

    TSchedulerEntityHandle Enroll(TString group, double weight, TMonotonic now);

    void AdvanceTime(TMonotonic now);

    void Deregister(TSchedulerEntity& self, TMonotonic now);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

struct TComputeActorSchedulingOptions {
    TMonotonic Now;
    NActors::TActorId NodeService;
    TSchedulerEntityHandle Handle;
    TComputeScheduler* Scheduler;
    TString Group = "";
    double Weight = 1;
    bool NoThrottle = true;
    TIntrusivePtr<TKqpCounters> Counters = nullptr;
};

struct TKqpComputeSchedulerEvents {
    enum EKqpComputeSchedulerEvents {
        EvDeregister = EventSpaceBegin(TKikimrEvents::ES_KQP) + 400,
        EvRenice,
        EvReniceConfirm,
        EvRedistribute
    };
};

struct TEvSchedulerDeregister : public TEventLocal<TEvSchedulerDeregister, TKqpComputeSchedulerEvents::EvDeregister> {
    TSchedulerEntityHandle SchedulerEntity;

    TEvSchedulerDeregister(TSchedulerEntityHandle entity)
        : SchedulerEntity(std::move(entity))
    {
    }
};

struct TEvSchedulerRenice : public TEventLocal<TEvSchedulerRenice, TKqpComputeSchedulerEvents::EvRenice> {
    TSchedulerEntityHandle SchedulerEntity;
    double DesiredWeight;
    TString DesiredGroup;

    TEvSchedulerRenice(TSchedulerEntityHandle handle, double weight, TString desiredGroup)
        : SchedulerEntity(std::move(handle))
        , DesiredWeight(weight)
        , DesiredGroup(desiredGroup)
    {
    }
};

struct TEvSchedulerReniceConfirm : public TEventLocal<TEvSchedulerReniceConfirm, TKqpComputeSchedulerEvents::EvReniceConfirm> {
    TSchedulerEntityHandle SchedulerEntity;

    TEvSchedulerReniceConfirm(TSchedulerEntityHandle entity)
        : SchedulerEntity(std::move(entity))
    {
    }
};


template<typename TDerived>
class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived> {
private:
    using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;

    static constexpr TDuration MaxDelay = TDuration::MilliSeconds(100);
    static constexpr TDuration ReniceTimeout = TDuration::Seconds(1);
    static constexpr TDuration MaxLag = TDuration::MilliSeconds(10);
    static constexpr TDuration MinDelay = TDuration::MilliSeconds(10);
    static constexpr double SecToUsec = 1e6;

public:
    template<typename... TArgs>
    TSchedulableComputeActorBase(TComputeActorSchedulingOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , SelfHandle(std::move(options.Handle))
        , NodeService(options.NodeService)
        , NoThrottle(options.NoThrottle)
        , Counters(options.Counters)
        , Group(options.Group)
        , Weight(options.Weight)
    {
        if (!NoThrottle) {
            //SelfHandle.TrackTime(Lag);
            if (Counters) {
                GroupUsage = Counters->GetKqpCounters()
                    ->GetSubgroup("NodeScheduler/Group", options.Group)
                    ->GetCounter("Usage", true);
            }
        }
    }

    static constexpr ui64 ResumeWakeupTag = 201;
    static constexpr ui64 ReniceWakeupTag = 202;

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        CA_LOG_D("wakeup with tag " << tag);
        if (tag == ResumeWakeupTag) {
            TBase::DoExecute();
        } else if (tag == ReniceWakeupTag) {
            Y_ABORT_UNLESS(SelfHandle);
            ReniceWakeupScheduled = false;
            auto now = TlsActivationContext->Monotonic();
            if (auto lag = SelfHandle.Lag(now)) {
                if (*lag >= MaxLag) {
                    return DoRenice(Min(Weight, SelfHandle.EstimateWeight(now, TDuration::MilliSeconds(1)) * 1.05));
                }
            }
            ScheduleReniceWakeup();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }

    void ScheduleReniceWakeup() {
        if (ReniceWakeupScheduled) {
            return;
        }
        this->Schedule(ReniceTimeout, new NActors::TEvents::TEvWakeup(ReniceWakeupTag));
        ReniceWakeupScheduled = true;
    }

    void DoRenice(double newWeight) {
        if (RunningRenice) {
            return;
        }
        RunningRenice = true;
        Counters->SchedulerRenices->Inc();
        auto renice = MakeHolder<TEvSchedulerRenice>(std::move(SelfHandle), newWeight, Group);
        this->Send(NodeService, renice.Release());
    }

    void HandleWork(TEvSchedulerReniceConfirm::TPtr& ev) {
        RunningRenice = false;
        SelfHandle = std::move(ev->Get()->SchedulerEntity);
        TBase::DoExecute();
    }

    STFUNC(BaseStateFuncBody) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
                hFunc(TEvSchedulerReniceConfirm, HandleWork);
                default:
                    TBase::BaseStateFuncBody(ev);
            }
        } catch (...) {
            CA_LOG_E("exception in CA handler " << CurrentExceptionMessage());
            PassAway();
        }
    }

private:
    void ReportThrottledTime(TMonotonic now) {
        if (Counters && Throttled) {
            Counters->SchedulerThrottled->Add((now - *Throttled).MicroSeconds());
            Throttled.Clear();
        }
    }

protected:
    void DoExecuteImpl() override {
        if (!SelfHandle) {// && Lag == TDuration::Zero()) {
            if (NoThrottle) {
                return TBase::DoExecuteImpl();
            } else {
                return DoRenice(Weight);
            }
        }

        ExecuteStart = NActors::TlsActivationContext->Monotonic();
        TMonotonic now = *ExecuteStart;
        TMaybe<TDuration> delay = CalcDelay(*ExecuteStart);
        bool executed = false;
        Counters->ScheduledActorsActivationsCount->Inc();
        if (NoThrottle || !delay) {
            ReportThrottledTime(now);
            executed = true;

            THPTimer timer;
            TBase::DoExecuteImpl();

            double passed = timer.Passed() * SecToUsec;

            if (Finished) {
                return;
            }
            SelfHandle.TrackTime(TDuration::MicroSeconds(passed));
            Counters->ScheduledActorsRuns->Collect(passed);
            if (GroupUsage) {
                GroupUsage->Add(passed);
            }
        }
        if (delay) {
            if (*delay > MaxDelay) {
                delay = MaxDelay;
            }
            CA_LOG_D("schedule wakeup after " << delay->MicroSeconds() << " msec ");
            Counters->SchedulerDelays->Collect(delay->MicroSeconds());
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup(ResumeWakeupTag));
        }

        if (!executed && delay) {
            Throttled = now;
        }
        ExecuteStart.Clear();
    }

    TMaybe<TDuration> CalcDelay(NMonotonic::TMonotonic now) {
        auto result = SelfHandle.CalcDelay(now);
        Counters->SchedulerVisibleLag->Collect(result.GetOrElse(TDuration::Zero()).MicroSeconds());
        if (NoThrottle || (result && *result < MinDelay)) {
            return {};
        } else {
            return result;
        }
    }

    void PassAway() override {
        Finished = true;
        if (ExecuteStart && SelfHandle) {
            SelfHandle.TrackTime(NActors::TlsActivationContext->Monotonic() - *ExecuteStart);
        }
        if (SelfHandle) {
            auto finishEv = MakeHolder<TEvSchedulerDeregister>(std::move(SelfHandle));
            this->Send(NodeService, finishEv.Release());
        }
        TBase::PassAway();
    }

private:
    TMaybe<TMonotonic> ExecuteStart;
    TMaybe<TMonotonic> Throttled;
    TSchedulerEntityHandle SelfHandle;
    NActors::TActorId NodeService;
    bool NoThrottle;
    bool Finished = false;

    TIntrusivePtr<TKqpCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr GroupUsage;

    TString Group;
    double Weight;

    bool ReniceWakeupScheduled = false;
    bool RunningRenice = false;

    //double Wdelta = 0;
};

} // namespace NKqp
} // namespace NKikimR
