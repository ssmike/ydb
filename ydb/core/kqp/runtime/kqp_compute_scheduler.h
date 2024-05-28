#pragma once

#include <util/datetime/base.h>


#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

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

    ~TSchedulerEntityHandle();
};

class TComputeScheduler {
public:
    TComputeScheduler();
    ~TComputeScheduler();

    void SetPriorities(THashMap<TString, double> priorities, double cores);

    TSchedulerEntityHandle Enroll(TString group, double weight);

    void AdvanceTime(TMonotonic now);

    void Deregister(TSchedulerEntity& self);

    void TrackTime(TSchedulerEntity& self, TDuration time);

    TMaybe<TDuration> CalcDelay(TSchedulerEntity& self, TMonotonic now);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

struct TComputeActorSchedulingOptions {
    NActors::TActorId NodeService;
    TComputeScheduler* Scheduler = nullptr;
    TString Group = "";
    double Weight = 1;
    bool NoThrottle = false;
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

public:
    template<typename... TArgs>
    TSchedulableComputeActorBase(TComputeActorSchedulingOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , Scheduler(options.Scheduler)
        , NoThrottle(options.NoThrottle)
    {
        if (Scheduler) {
            SelfHandle = Scheduler->Enroll(options.Group, options.Weight);
        }
    }

    static constexpr ui64 ResumeWakeupTag = 201;

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        if (tag == ResumeWakeupTag) {
            //TBase::Start();
            TBase::DoExecute();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }


    STFUNC(BaseStateFuncBody) {
        switch (ev->GetTypeRewrite()) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
                default:
                    TBase::BaseStateFuncBody(ev);
            }
        }
    }

protected:
    void DoExecuteImpl() override {
        auto start = NActors::TlsActivationContext->Monotonic();
        TMaybe<TDuration> delay;
        if (SelfHandle) {
            delay = Scheduler->CalcDelay(*SelfHandle, start);
        }
        TMonotonic now;
        if (NoThrottle || !delay) {
            //auto* stats = TBase::GetTaskRunnerStats();
            TBase::DoExecuteImpl();
            now = NActors::TlsActivationContext->Monotonic();
            Scheduler->TrackTime(*SelfHandle, now - start);
            delay = Scheduler->CalcDelay(*SelfHandle, now);
        } else {
            now = NActors::TlsActivationContext->Monotonic();
        }
        if (delay) {
            //TBase::Stop();
            this->Schedule(now + *delay, new NActors::TEvents::TEvWakeup(ResumeWakeupTag));
        }
    }

    void PassAway() override {
        auto finishEv = MakeHolder<TEvFinishKqpTask>(std::get<ui64>(this->GetTxId()), this->GetTask().GetId(), true);
        finishEv->SchedulerEntity = std::move(SelfHandle);
        this->Send(NodeService, finishEv.Release());
        TBase::PassAway();
    }

private:
    TComputeScheduler* Scheduler;
    TSchedulerEntityHandle SelfHandle;
    NActors::TActorId NodeService;
    bool NoThrottle;
};

} // namespace NKqp
} // namespace NKikimR
