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

    void TrackTime(TSchedulerEntity& self, TDuration time);

    double GroupNow(TSchedulerEntity& self, TMonotonic now);

    TMaybe<TDuration> CalcDelay(TSchedulerEntity& self, TMonotonic now);

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

struct TEvFinishKqpTask : public TEventLocal<TEvFinishKqpTask, TKqpEvents::EKqpEvents::EvFinishKqpTasks> {
    const ui64 TxId;
    const ui64 TaskId;
    const bool Success;
    const NYql::TIssues Issues;

    TSchedulerEntityHandle SchedulerEntity;

    TEvFinishKqpTask(ui64 txId, ui64 taskId, bool success, const NYql::TIssues& issues = {})
        : TxId(txId)
        , TaskId(taskId)
        , Success(success)
        , Issues(issues) {}
};


template<typename TDerived>
class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived> {
private:
    using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;

    static constexpr TDuration MaxDelay = TDuration::MilliSeconds(60);

public:
    template<typename... TArgs>
    TSchedulableComputeActorBase(TComputeActorSchedulingOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , Scheduler(options.Scheduler)
        , NoThrottle(options.NoThrottle)
    {
        if (Scheduler) {
            SelfHandle = Scheduler->Enroll(options.Group, options.Weight, options.Now);
        }
        if (options.Counters) {
            ThrottledCounter = options.Counters->SchedulerThrottled;
        }
    }

    static constexpr ui64 ResumeWakeupTag = 201;

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        CA_LOG_D("wakeup with tag " << tag);
        if (tag == ResumeWakeupTag) {
            //TBase::Start();
            TBase::DoExecute();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }


    STFUNC(BaseStateFuncBody) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
            default:
                TBase::BaseStateFuncBody(ev);
        }
    }

private:
    void ReportThrottledTime(TMonotonic now) {
        if (ThrottledCounter && Throttled) {
            ThrottledCounter->Add((now - *Throttled).MicroSeconds());
            Throttled.Clear();
        }
    }

protected:
    void DoExecuteImpl() override {
        if (!SelfHandle) {
            return TBase::DoExecuteImpl();
        }

        ExecuteStart = NActors::TlsActivationContext->Monotonic();
        TMonotonic now = *ExecuteStart;
        TMaybe<TDuration> delay = Scheduler->CalcDelay(*SelfHandle, *ExecuteStart);
        bool executed = false;
        if (NoThrottle || !delay) {
            ReportThrottledTime(now);
            executed = true;

            TBase::DoExecuteImpl();
            if (Finished) {
                return;
            }
            now = NActors::TlsActivationContext->Monotonic();
            Scheduler->TrackTime(*SelfHandle, now - *ExecuteStart);
            delay = Scheduler->CalcDelay(*SelfHandle, now);
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
            Scheduler->TrackTime(*SelfHandle, NActors::TlsActivationContext->Monotonic() - *ExecuteStart);
        }
        auto finishEv = MakeHolder<TEvFinishKqpTask>(std::get<ui64>(this->GetTxId()), this->GetTask().GetId(), true);
        finishEv->SchedulerEntity = std::move(SelfHandle);
        this->Send(NodeService, finishEv.Release());
        TBase::PassAway();
    }

private:
    TMaybe<TMonotonic> ExecuteStart;
    TMaybe<TMonotonic> Throttled;
    TComputeScheduler* Scheduler;
    TSchedulerEntityHandle SelfHandle;
    NActors::TActorId NodeService;
    bool NoThrottle;
    bool Finished = false;

    ::NMonitoring::TDynamicCounters::TCounterPtr ThrottledCounter;
};

} // namespace NKqp
} // namespace NKikimR
