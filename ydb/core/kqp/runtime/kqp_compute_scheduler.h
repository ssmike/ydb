#pragma once

#include <util/datetime/base.h>


#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr {
namespace NKqp {

template<typename T>
class TMultiThreadView {
public:
    TMultiThreadView(std::atomic<ui64>* usage, T* slot)
        : Usage(usage)
        , Slot(slot)
    {
        Usage->fetch_add(1);
    }

    const T* get() {
        return Slot;
    }

    ~TMultiThreadView() {
        Usage->fetch_sub(1);
    }

private:
    std::atomic<ui64>* Usage;
    T* Slot;
};

template<typename T>
class TMultithreadPublisher {
public:
    void Publish() {
        auto oldVal = CurrentT.load();
        auto newVal = 1 - oldVal;
        CurrentT.store(newVal);
        while (true) {
            if (Usage[oldVal].load() == 0) {
                Slots[oldVal] = Slots[newVal];
                return;
            }
        }
    }

    T* Next() {
        return &Slots[1 - CurrentT.load()];
    }

    TMultiThreadView<T> Current() {
        while (true) {
            auto val = CurrentT.load();
            TMultiThreadView<T> view(&Usage[val], &Slots[val]);
            if (CurrentT.load() == val) {
                return view;
            }
        }
    }

private:
    std::atomic<ui32> CurrentT = 0;
    std::atomic<ui64> Usage[2] = {0, 0};
    T Slots[2];
};

class TSchedulerEntityHandle {
public:
    TSchedulerEntityHandle() {}
    ~TSchedulerEntityHandle() {}

private:
    friend class TComputeScheduler;

    struct TGroupRecord {
        double Weight;
        double Now = 0;
        TMonotonic LastNowRecalc;
        bool Disabled = false;
        double EntitiesWeight = 0;
    };

    TMultithreadPublisher<TGroupRecord>* Group;
    double Weight;
    double Vruntime = 0;
    double Vstart;
};

class TComputeScheduler {
private:
    static constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }

    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr double MinPriority = 1e-8;
public:
    TComputeScheduler(){}

    void SetPriorities(THashMap<TString, double> priorities, double cores) {
        double sum = 0;
        for (auto [_, v] : priorities) {
            sum += v;
        }
        for (auto& [k, v] : Groups) {
            auto ptr = priorities.FindPtr(k);
            if (ptr) {
                v->Next()->Weight = ((*ptr) * cores) / sum;
                v->Next()->Disabled = false;
            } else {
                v->Next()->Weight = 0;
                v->Next()->Disabled = true;
            }
            v->Publish();
        }
        for (auto& [k, v] : priorities) {
            if (!Groups.contains(k)) {
                auto group = std::make_unique<TMultithreadPublisher<TSchedulerEntityHandle::TGroupRecord>>();
                group->Next()->LastNowRecalc = TMonotonic::Now();
                group->Next()->Weight = (v * cores) / sum;
                group->Publish();
                Groups[k] = std::move(group);
            }
        }
    }

    std::unique_ptr<TSchedulerEntityHandle> Enroll(TString group, double weight) {
        Y_ENSURE(Groups.contains(group), "unknown scheduler group");
        auto* groupEntry = Groups[group].get();
        auto result = std::make_unique<TSchedulerEntityHandle>();
        result->Group = groupEntry;
        result->Weight = weight;
        result->Vstart = groupEntry->Next()->Now;
        groupEntry->Next()->EntitiesWeight += weight;
        return result;
    }

    void AdvanceTime(TMonotonic now) {
        for (auto& [_, v] : Groups) {
            {
                auto group = v.get()->Current();
                if (!group.get()->Disabled && group.get()->EntitiesWeight > MinPriority) {
                    v.get()->Next()->Now += FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
                    v.get()->Next()->LastNowRecalc = now;
                }
            }
            v->Publish();
        }
    }

    void Deregister(TSchedulerEntityHandle& self) {
        auto* group = self.Group->Next();
        group->Weight -= self.Weight;
    }

    void TrackTime(TSchedulerEntityHandle& self, TDuration time) {
        self.Vruntime += FromDuration(time) / self.Weight;
    }

    TMaybe<TDuration> CalcDelay(TSchedulerEntityHandle& self, TMonotonic now) {
        auto group = self.Group->Current();
        Y_ENSURE(!group.get()->Disabled);
        auto lagTime = (self.Vruntime - (group.get()->Now - self.Vstart)) * group.get()->EntitiesWeight / group.get()->Weight;
        auto neededTime = lagTime - FromDuration(now - group.get()->LastNowRecalc);
        if (neededTime <= 0) {
            return Nothing();
        } else {
            return ToDuration(neededTime);
        }
    }

private:
    THashMap<TString, std::unique_ptr<TMultithreadPublisher<TSchedulerEntityHandle::TGroupRecord>>> Groups;
};

struct TComputeActorCpuPriorityOptions {
    TComputeScheduler* Scheduler = nullptr;
    TString Group = "";
    double Weight = 1;
    NActors::TActorId NodeService;
};

struct TEvFinishKqpTask : public TEventLocal<TEvFinishKqpTask, TKqpEvents::EKqpEvents::EvFinishKqpTasks> {
    const ui64 TxId;
    const ui64 TaskId;
    const bool Success;
    const NYql::TIssues Issues;

    std::unique_ptr<TSchedulerEntityHandle> SchedulerEntity;

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
    TSchedulableComputeActorBase(TArgs&&... args, TComputeActorCpuPriorityOptions options = {})
        : TBase(std::forward<TArgs>(args)...)
        , Scheduler(options.Scheduler)
    {
        if (Scheduler) {
            SelfHandle = Scheduler->Enroll(options.Group, options.Weight);
        }
    }

protected:
    void DoExecuteImpl() override {
        auto start = NActors::TlsActivationContext->Monotonic();
        auto* stats = TBase::GetTaskRunnerStats();
        TBase::DoExecuteImpl();
        Scheduler->TrackTime(*SelfHandle, NActors::TlsActivationContext->Monotonic() - start);
    }

    void PassAway() override {
        auto finishEv = MakeHolder<TEvFinishKqpTask>();
        finishEv->TxId = this->GetTxId();
        finishEv->TaskId = this->GetTask().GetId();
        finishEv->SchedulerEntity = std::move(SelfHandle);
        this->Send(NodeService, finishEv.Release());
        TBase::PassAway();
    }

private:
    TComputeScheduler* Scheduler;
    std::unique_ptr<TSchedulerEntityHandle> SelfHandle;
    NActors::TActorId NodeService;
};

} // namespace NKqp
} // namespace NKikimR
