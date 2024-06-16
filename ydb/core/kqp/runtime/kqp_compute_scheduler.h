#pragma once

#include <util/datetime/base.h>

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

    double GroupNow(TSchedulerEntity& self, TMonotonic now);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

struct TComputeActorSchedulingOptions {
    TMonotonic Now;
    NActors::TActorId NodeService;
    TComputeScheduler* Scheduler = nullptr;
    TString Group = "";
    double Weight = 1;
    bool NoThrottle = false;
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

struct TEvSchedulerReniceConfirm : public TEventLocal<TEvSchedulerRenice, TKqpComputeSchedulerEvents::EvReniceConfirm> {
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

    static constexpr TDuration MaxDelay = TDuration::MilliSeconds(60);
    static constexpr TDuration ReniceTimeout = TDuration::MilliSeconds(120);

public:
    template<typename... TArgs>
    TSchedulableComputeActorBase(TComputeActorSchedulingOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , NoThrottle(options.NoThrottle)
        , Counters(options.Counters)
        , Group(options.Group)
        , Weight(options.Weight)
    {
        if (!NoThrottle) {
            Y_ENSURE(options.Scheduler);
            SelfHandle = options.Scheduler->Enroll(options.Group, options.Weight, options.Now);
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
            auto renice = MakeHolder<TEvSchedulerRenice>(std::move(SelfHandle), Group, Weight);
            this->Send(NodeService, renice.Release());
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }

    void Bootstrap() override {
        if (!NoThrottle) {
            this->Schedule(ReniceTimeout, new NActors::TEvents::TEvWakeup(ReniceWakeupTag));
        }
        TBase::Bootstrap();
    }

    void HandleWork(TEvSchedulerReniceConfirm::TPtr& ev) {
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
        if (!SelfHandle) {
            if (NoThrottle) {
                return TBase::DoExecuteImpl();
            } else {
                return;
            }
        }

        ExecuteStart = NActors::TlsActivationContext->Monotonic();
        TMonotonic now = *ExecuteStart;
        TMaybe<TDuration> delay = SelfHandle.CalcDelay(*ExecuteStart);
        bool executed = false;
        if (NoThrottle || !delay) {
            ReportThrottledTime(now);
            executed = true;

            TBase::DoExecuteImpl();
            if (Finished) {
                return;
            }
            now = NActors::TlsActivationContext->Monotonic();
            SelfHandle.TrackTime(now - *ExecuteStart);
            if (GroupUsage) {
                GroupUsage->Add((now - *ExecuteStart).MicroSeconds());
            }
            delay = SelfHandle.CalcDelay(now);
        }
        if (delay) {
            if (*delay > MaxDelay) {
                delay = MaxDelay;
            }
            CA_LOG_D("schedule wakeup after " << delay->MicroSeconds() << " msec ");
            this->Schedule(now + *delay, new NActors::TEvents::TEvWakeup(ResumeWakeupTag));
        }

        if (executed && delay) {
            Throttled = now;
        }
        ExecuteStart.Clear();
    }

    void PassAway() override {
        Finished = true;
        if (ExecuteStart && SelfHandle) {
            SelfHandle.TrackTime(NActors::TlsActivationContext->Monotonic() - *ExecuteStart);
        }
        auto finishEv = MakeHolder<TEvSchedulerDeregister>(std::move(SelfHandle));
        this->Send(NodeService, finishEv.Release());
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
};

} // namespace NKqp
} // namespace NKikimR
