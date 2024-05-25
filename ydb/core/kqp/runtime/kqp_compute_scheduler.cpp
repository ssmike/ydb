#include "kqp_compute_scheduler.h"

namespace {
    static constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }

    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr double MinPriority = 1e-8;

}

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

TSchedulerEntityHandle::~TSchedulerEntityHandle() = default;

class TSchedulerEntity {
public:
    TSchedulerEntity() {}
    ~TSchedulerEntity() {}

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

struct TComputeScheduler::TImpl {
    THashMap<TString, std::unique_ptr<TMultithreadPublisher<TSchedulerEntity::TGroupRecord>>> Groups;
};

TComputeScheduler::TComputeScheduler() {
    Impl = std::make_unique<TImpl>();
}

TComputeScheduler::~TComputeScheduler() = default;

void TComputeScheduler::SetPriorities(THashMap<TString, double> priorities, double cores) {
    double sum = 0;
    for (auto [_, v] : priorities) {
        sum += v;
    }
    for (auto& [k, v] : Impl->Groups) {
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
        if (!Impl->Groups.contains(k)) {
            auto group = std::make_unique<TMultithreadPublisher<TSchedulerEntity::TGroupRecord>>();
            group->Next()->LastNowRecalc = TMonotonic::Now();
            group->Next()->Weight = (v * cores) / sum;
            group->Publish();
            Impl->Groups[k] = std::move(group);
        }
    }
}

TSchedulerEntityHandle TComputeScheduler::Enroll(TString group, double weight) {
    Y_ENSURE(Impl->Groups.contains(group), "unknown scheduler group");
    auto* groupEntry = Impl->Groups[group].get();
    auto result = std::make_unique<TSchedulerEntity>();
    result->Group = groupEntry;
    result->Weight = weight;
    result->Vstart = groupEntry->Next()->Now;
    groupEntry->Next()->EntitiesWeight += weight;
    return TSchedulerEntityHandle(std::move(result));
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    for (auto& [_, v] : Impl->Groups) {
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

void TComputeScheduler::Deregister(TSchedulerEntity& self) {
    auto* group = self.Group->Next();
    group->Weight -= self.Weight;
}

void TComputeScheduler::TrackTime(TSchedulerEntity& self, TDuration time) {
    self.Vruntime += FromDuration(time) / self.Weight;
}

TMaybe<TDuration> TComputeScheduler::CalcDelay(TSchedulerEntity& self, TMonotonic now) {
    auto group = self.Group->Current();
    Y_ENSURE(!group.get()->Disabled);
    double lagTime = (self.Vruntime - (group.get()->Now - self.Vstart)) * group.get()->EntitiesWeight / group.get()->Weight;
    double neededTime = lagTime - FromDuration(now - group.get()->LastNowRecalc);
    if (neededTime <= 0) {
        return Nothing();
    } else {
        return ToDuration(neededTime);
    }
}


} // namespace NKqp
} // namespace NKikimr
