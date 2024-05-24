#pragma once

#include <util/datetime/base.h>


#include <ydb/library/actors/core/actor_bootstrapped.h>

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
    friend class TComputeScheduler;

    TSchedulerEntityHandle(){}

    struct TGroupRecord {
        double Weight;
        double Now = 0;
        TInstant LastNowRecalc;
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
    TComputeScheduler(double cores)
        : CoresScaleFactor(cores)
    {}

    void SetPriorities(THashMap<TString, double> priorities) {
        double sum = 0;
        for (auto [_, v] : priorities) {
            sum += v;
        }
        for (auto& [k, v] : Groups) {
            auto ptr = priorities.FindPtr(k);
            if (ptr) {
                v->Next()->Weight = ((*ptr) * CoresScaleFactor) / sum;
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
                group->Next()->LastNowRecalc = TInstant::Now();
                group->Next()->Weight = (v * CoresScaleFactor) / sum;
                group->Publish();
                Groups[k] = std::move(group);
            }
        }
    }

    TSchedulerEntityHandle Enroll(TString group, double weight) {
        Y_ENSURE(Groups.contains(group), "unknown scheduler group");
        auto* groupEntry = Groups[group].get();
        TSchedulerEntityHandle result;
        result.Group = groupEntry;
        result.Weight = weight;
        result.Vstart = groupEntry->Next()->Now;
        groupEntry->Next()->EntitiesWeight += weight;
        return result;
    }

    void RecalcNow(TInstant now) {
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

    TMaybe<TDuration> CalcDelay(TSchedulerEntityHandle& self, TInstant now) {
        auto group = self.Group->Current();
        Y_ENSURE(!group.get()->Disabled);
        auto lagTime = (self.Vruntime - group.get()->Now) * group.get()->EntitiesWeight / group.get()->Weight;
        auto neededTime = lagTime - FromDuration(now - group.get()->LastNowRecalc);
        if (neededTime <= 0) {
            return Nothing();
        } else {
            return ToDuration(neededTime);
        }
    }

private:
    THashMap<TString, std::unique_ptr<TMultithreadPublisher<TSchedulerEntityHandle::TGroupRecord>>> Groups;
    double CoresScaleFactor;
};

} // namespace NKqp
} // namespace NKikimr
