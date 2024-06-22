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

    struct TGroupMutableStats {
        double Weight = 0;
        double Now = 0;
        TMonotonic LastNowRecalc;
        bool Disabled = false;
        double EntitiesWeight = 0;

        size_t AllowTrackUsec = 0;

        double GroupNow(TMonotonic now) const {
            if (EntitiesWeight < MinEntitiesWeight) {
                return Now;
            } else {
                return Now + FromDuration(now - LastNowRecalc) * Weight / EntitiesWeight;
            }
        }
    };

    struct TGroupRecord {
        TAtomic TrackedMicroSeconds;
        TAtomic Delayed = 0;
        TMultithreadPublisher<TGroupMutableStats> MutableStats;
    };

    TGroupRecord* Group;
    double Weight;
    double Vruntime = 0;
    double Vstart;

    void TrackTime(TDuration time) {
        Vruntime += FromDuration(time) / Weight;
        AtomicAdd(Group->TrackedMicroSeconds, time.MicroSeconds());
    }

    TMaybe<TDuration> GroupDelay(TMonotonic now) {
        auto group = Group->MutableStats.Current();
        auto limit = group.get()->AllowTrackUsec + (now - group.get()->LastNowRecalc).MicroSeconds() * group.get()->Weight;
        if (limit < Group->TrackedMicroSeconds) {
            return {};
        } else {
        }
    }

    void SetEnabled(TMonotonic /* now */, bool enabled) {
        if (enabled) {
            AtomicIncrement(Group->Delayed);
        } else {
            AtomicDecrement(Group->Delayed);
        }
    }

    TMaybe<TDuration> CalcDelay(TMonotonic now) {
        auto group = Group->MutableStats.Current();
        Y_ENSURE(!group.get()->Disabled);
        double lag = Vruntime - (group.get()->GroupNow(now) - Vstart);
        if (lag <= 0) {
            return Nothing();
        } else {
            return ToDuration(lag * group.get()->EntitiesWeight / group.get()->Weight);
        }
        //double lagTime = (Vruntime - (group.get()->Now - Vstart)) * group.get()->EntitiesWeight / group.get()->Weight;
        //double neededTime = lagTime - FromDuration(now - group.get()->LastNowRecalc);
        //if (neededTime <= 0) {
        //    return Nothing();
        //} else {
        //    return ToDuration(neededTime);
        //}
    }

    TMaybe<TDuration> Lag(TMonotonic now) {
        auto group = Group->MutableStats.Current();
        Y_ENSURE(!group.get()->Disabled);
        double lagTime = (group.get()->GroupNow(now) - Vstart - Vruntime) * Weight;
        if (lagTime <= 0) {
            return Nothing();
        } else {
            return ToDuration(lagTime);
        }
    }

    //double LagVTime(TMonotonic now) {
    //    auto group = Group->MutableStats.Current();
    //    return FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight + group.get()->Now - (Vruntime - Vstart)
    //}

    double EstimateWeight(TMonotonic now, TDuration minTime) {
        double vruntime = Max(Vruntime, FromDuration(minTime) / Weight);
        double vtime = Group->MutableStats.Current().get()->GroupNow(now);
        return Weight * (vruntime / vtime);
    }
};

double TSchedulerEntityHandle::VRuntime() {
    return Ptr->Vruntime;
}

struct TComputeScheduler::TImpl {
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> VtimeCounters;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> EntitiesWeightCounters;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> LimitCounters;

    THashMap<TString, size_t> PoolId;
    std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> Records;

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

    TIntrusivePtr<TKqpCounters> Counters;

    void AssignWeights() {
        ssize_t rootRule = static_cast<ssize_t>(Rules.size()) - 1;
        for (size_t i = 0; i < Rules.size(); ++i) {
            Rules[i].SubRulesSum = 0;
            Rules[i].Empty = true;
        }
        for (ssize_t i = 0; i < static_cast<ssize_t>(Rules.size()); ++i) {
            if (Rules[i].RecordId) {
                Rules[i].Empty = Records[*Rules[i].RecordId]->MutableStats.Next()->EntitiesWeight < MinEntitiesWeight;
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
                Records[*Rules[i].RecordId]->MutableStats.Next()->Weight = Rules[i].Weight;
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
            auto group = std::make_unique<TSchedulerEntity::TGroupRecord>();
            group->MutableStats.Next()->LastNowRecalc = now;
            Impl->Records.push_back(std::move(group));
        }
    }
    for (auto& [k, v] : Impl->PoolId) {
        if (!seenNames.contains(k)) {
            auto& group = Impl->Records[Impl->PoolId[k]]->MutableStats;
            group.Next()->Weight = 0;
            group.Next()->Disabled = true;
            group.Publish();
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
        record->MutableStats.Publish();
    }
}


TSchedulerEntityHandle TComputeScheduler::Enroll(TString groupName, double weight, TMonotonic now) {
    Y_ENSURE(Impl->PoolId.contains(groupName), "unknown scheduler group");
    auto* groupEntry = Impl->Records[Impl->PoolId.at(groupName)].get();
    auto result = std::make_unique<TSchedulerEntity>();
    result->Group = groupEntry;
    result->Weight = weight;
    result->Vstart = groupEntry->MutableStats.Next()->Now;
    groupEntry->MutableStats.Next()->EntitiesWeight += weight;

    Impl->AssignWeights();
    AdvanceTime(now);
    return TSchedulerEntityHandle(result.release());
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    if (Impl->Counters) {
        if (Impl->VtimeCounters.size() < Impl->Records.size()) {
            Impl->VtimeCounters.resize(Impl->Records.size());
            Impl->EntitiesWeightCounters.resize(Impl->Records.size());
            Impl->LimitCounters.resize(Impl->Records.size());
            for (auto& [k, i] : Impl->PoolId) {
                Impl->VtimeCounters[i] = Impl->Counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", k)->GetCounter("VTime", true);
                Impl->EntitiesWeightCounters[i] = Impl->Counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", k)->GetCounter("Entities", false);
                Impl->LimitCounters[i] = Impl->Counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", k)->GetCounter("Limit", true);
            }
        }
    }
    for (size_t i = 0; i < Impl->Records.size(); ++i) {
        auto& v = Impl->Records[i]->MutableStats;
        {
            auto group = v.Current();
            double delta = 0;
            if (!group.get()->Disabled && group.get()->EntitiesWeight > MinEntitiesWeight) {
                v.Next()->Now += delta = FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
            }
            v.Next()->LastNowRecalc = now;
            // Cerr << v->Next()->EntitiesWeight << " entities " << v->Next()->Weight << " weight" << Endl;
            if (Impl->VtimeCounters.size() > i && Impl->VtimeCounters[i]) {
                Impl->VtimeCounters[i]->Add(delta);
                Impl->EntitiesWeightCounters[i]->Set(v.Next()->EntitiesWeight);
                Impl->LimitCounters[i]->Add(FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight);
            }
        }
        v.Publish();
    }
}

void TComputeScheduler::Deregister(TSchedulerEntity& self, TMonotonic now) {
    auto* group = self.Group->MutableStats.Next();
    group->EntitiesWeight -= self.Weight;

    double delta = self.Group->MutableStats.Current().get()->GroupNow(now) - self.Vruntime;
    if (group->EntitiesWeight > MinEntitiesWeight) {
        group->Now += delta * self.Weight / group->EntitiesWeight;
    }

    Impl->AssignWeights();
    AdvanceTime(now);
}

void TSchedulerEntityHandle::TrackTime(TDuration time) {
    Ptr->TrackTime(time);
}

TMaybe<TDuration> TSchedulerEntityHandle::CalcDelay(TMonotonic now) {
    return Ptr->CalcDelay(now);
}

TMaybe<TDuration> TSchedulerEntityHandle::GroupDelay(TMonotonic now) {
    return Ptr->GroupDelay(now);
}

void TSchedulerEntityHandle::SetEnabled(TMonotonic now, bool enabled) {
    Ptr->SetEnabled(now, enabled);
}

TMaybe<TDuration> TSchedulerEntityHandle::Lag(TMonotonic now) {
    return Ptr->Lag(now);
}

//double TSchedulerEntityHandle::LagVTime(TMonotonic now) {
//    return Ptr->LagVTime(now);
//}

double TSchedulerEntityHandle::GroupNow(TMonotonic now) {
    return Ptr->Group->MutableStats.Current().get()->GroupNow(now);
}

void TSchedulerEntityHandle::Clear() {
    Ptr.reset();
}

double TSchedulerEntityHandle::EstimateWeight(TMonotonic now, TDuration minTime) {
    return Ptr->EstimateWeight(now, minTime);
}

void TComputeScheduler::ReportCounters(TIntrusivePtr<TKqpCounters> counters) {
    Impl->Counters = counters;
}

} // namespace NKqp
} // namespace NKikimr
