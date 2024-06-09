#include "kqp_compute_scheduler.h"

namespace {
    static constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }

    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr double MinEntitiesWeight = 1e-8;

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

TSchedulerEntityHandle::TSchedulerEntityHandle(TSchedulerEntity* ptr)
    : Ptr(ptr)
{
}

TSchedulerEntityHandle::TSchedulerEntityHandle(){} 

TSchedulerEntityHandle::TSchedulerEntityHandle(TSchedulerEntityHandle&& other)
    : Ptr(other.Ptr.release())
{
}

TSchedulerEntityHandle& TSchedulerEntityHandle::operator = (TSchedulerEntityHandle&& other) {
    Ptr.swap(other.Ptr);
    return *this;
}

TSchedulerEntityHandle::~TSchedulerEntityHandle() = default;

class TSchedulerEntity {
public:
    TSchedulerEntity() {}
    ~TSchedulerEntity() {}

private:
    friend class TComputeScheduler;
    friend class TSchedulerEntityHandle;

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

double TSchedulerEntityHandle::VRuntime() {
    return Ptr->Vruntime;
}

struct TComputeScheduler::TImpl {
    THashMap<TString, size_t> PoolId;
    std::vector<std::unique_ptr<TMultithreadPublisher<TSchedulerEntity::TGroupRecord>>> Records;

    struct TRule {
        size_t Parent;
        double Weight = 0;

        double Share;
        TMaybe<size_t> RecordId = {};
        double SubRulesSum = 0;
        bool Empty = true;
    };
    std::vector<TRule> Rules;

    double SumCores;

    void AssignWeights() {
        ssize_t rootRule = static_cast<ssize_t>(Rules.size()) - 1;
        for (size_t i = 0; i < Rules.size(); ++i) {
            Rules[i].SubRulesSum = 0;
            Rules[i].Empty = true;
        }
        for (ssize_t i = 0; i < static_cast<ssize_t>(Rules.size()); ++i) {
            if (Rules[i].RecordId) {
                Rules[i].Empty = Records[*Rules[i].RecordId]->Next()->EntitiesWeight < MinEntitiesWeight;
                Rules[i].SubRulesSum = Rules[i].Share;
            }
            if (i != rootRule && !Rules[i].Empty) {
                Rules[Rules[i].Parent].Empty = false;
                Rules[Rules[i].Parent].SubRulesSum += Rules[i].SubRulesSum;
            }
        }
        for (ssize_t i = static_cast<ssize_t>(Rules.size()) - 1; i >= 0; --i) {
            if (i == static_cast<ssize_t>(Rules.size()) - 1) {
                Rules[i].Weight = SumCores * Rules[i].Share;
            } else if (!Rules[i].Empty) {
                Rules[i].Weight = Rules[Rules[i].Parent].Weight * Rules[i].Share / Rules[Rules[i].Parent].SubRulesSum;
            } else {
                Rules[i].Weight = 0;
            }
            if (Rules[i].RecordId) {
                Records[*Rules[i].RecordId]->Next()->Weight = Rules[i].Weight;
            }
        }
     }
};

TComputeScheduler::TComputeScheduler() {
    Impl = std::make_unique<TImpl>();
}

TComputeScheduler::~TComputeScheduler() = default;

void TComputeScheduler::SetPriorities(TDistributionRule rule, double cores, TMonotonic now) {
    THashSet<TString> seenNames;
    std::function<void(TDistributionRule&)> exploreNames = [&](TDistributionRule& rule) {
        if (rule.SubRules.empty()) {
            seenNames.insert(rule.Name);
        } else {
            for (auto& subRule : rule.SubRules) {
                exploreNames(subRule);
            }
        }
    };
    exploreNames(rule);

    for (auto& k : seenNames) {
        auto ptr = Impl->PoolId.FindPtr(k);
        if (!ptr) {
            Impl->PoolId[k] = Impl->Records.size();
            auto group = std::make_unique<TMultithreadPublisher<TSchedulerEntity::TGroupRecord>>();
            group->Next()->LastNowRecalc = now;
            Impl->Records.push_back(std::move(group));
        }
    }
    for (auto& [k, v] : Impl->PoolId) {
        if (!seenNames.contains(k)) {
            auto* group = Impl->Records[Impl->PoolId[k]].get();
            group->Next()->Weight = 0;
            group->Next()->Disabled = true;
            group->Publish();
        }
    }
    Impl->SumCores = cores;

    TVector<TImpl::TRule> rules;
    std::function<size_t(TDistributionRule&)> makeRules = [&](TDistributionRule& rule) {
        size_t result;
        if (rule.SubRules.empty()) {
            result = rules.size();
            rules.push_back(TImpl::TRule{.Share = rule.Share, .RecordId=Impl->PoolId[rule.Name]});
        } else {
            TVector<size_t> toAssign;
            for (auto& subRule : rule.SubRules) {
                toAssign.push_back(makeRules(subRule));
            }
            size_t result = rules.size();
            rules.push_back(TImpl::TRule{.Share = rule.Share});
            for (auto i : toAssign) {
                rules[i].Parent = result;
            }
            return result;
        }
        return result;
    };
    makeRules(rule);
    Impl->Rules.swap(rules);

    Impl->AssignWeights();
    for (auto& record : Impl->Records) {
        record->Publish();
    }
}


double TComputeScheduler::GroupNow(TSchedulerEntity& self, TMonotonic now) {
    auto group = self.Group->Current();
    if (group.get()->EntitiesWeight < MinEntitiesWeight) {
        return group.get()->Now;
    } else {
        return group.get()->Now + FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
    }
}


TSchedulerEntityHandle TComputeScheduler::Enroll(TString groupName, double weight, TMonotonic now) {
    Y_ENSURE(Impl->PoolId.contains(groupName), "unknown scheduler group");
    auto* groupEntry = Impl->Records[Impl->PoolId.at(groupName)].get();
    auto result = std::make_unique<TSchedulerEntity>();
    result->Group = groupEntry;
    result->Weight = weight;
    result->Vstart = groupEntry->Current().get()->Now;
    groupEntry->Next()->EntitiesWeight += weight;

    Impl->AssignWeights();
    AdvanceTime(now);
    return TSchedulerEntityHandle(result.release());
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    for (auto& v : Impl->Records) {
        {
            auto group = v.get()->Current();
            if (!group.get()->Disabled && group.get()->EntitiesWeight > MinEntitiesWeight) {
                v.get()->Next()->Now += FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
            }
            v.get()->Next()->LastNowRecalc = now;
        }
        v->Publish();
    }
}

void TComputeScheduler::Deregister(TSchedulerEntity& self, TMonotonic now) {
    auto* group = self.Group->Next();
    group->Weight -= self.Weight;

    Impl->AssignWeights();
    AdvanceTime(now);
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
