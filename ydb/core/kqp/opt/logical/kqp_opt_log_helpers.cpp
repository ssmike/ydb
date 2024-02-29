#include "kqp_opt_log_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TExprBase MakeLT(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoAnd>()
            .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
            .Add<TCoExists>().Optional(right).Build()
            .Build()
        .Add<TCoCmpLess>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

TExprBase MakeEQ(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoAnd>()
            .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
            .Add<TCoNot>().Value<TCoExists>().Optional(right).Build().Build()
            .Build()
        .Add<TCoCmpEqual>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

TExprBase MakeLE(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
        .Add<TCoCmpLessOrEqual>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

} // namespace

TCoLambda MakeFilterForRange(TKqlKeyRange range, TExprContext& ctx, TPositionHandle pos, TVector<TString> keyColumns) {
    size_t prefix = 0;
    auto arg = Build<TCoArgument>(ctx, pos).Name("_row_arg").Done();
    TVector<TExprBase> conds;
    while (prefix < range.From().ArgCount() && prefix < range.To().ArgCount()) {
        auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[prefix]).Done();
        if (range.From().Arg(prefix).Raw() == range.To().Arg(prefix).Raw()) {
            if (prefix + 1 == range.From().ArgCount() && range.From().Maybe<TKqlKeyExc>()) {
                break;
            }
            if (prefix + 1 == range.To().ArgCount() && range.To().Maybe<TKqlKeyExc>()) {
                break;
            }
        } else {
            break;
        }
        conds.push_back(MakeEQ(column, range.From().Arg(prefix), ctx, pos));
        prefix += 1;
    }

    {
        TMaybeNode<TExprBase> tupleComparison;
        for (ssize_t i = static_cast<ssize_t>(range.From().ArgCount()) - 1; i >= static_cast<ssize_t>(prefix); --i) {
            auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[i]).Done();
            if (tupleComparison.IsValid()) {
                tupleComparison = Build<TCoOr>(ctx, pos)
                    .Add(MakeLT(range.From().Arg(i), column, ctx, pos))
                    .Add<TCoAnd>()
                        .Add(MakeEQ(range.From().Arg(i), column, ctx, pos))
                        .Add(tupleComparison.Cast())
                        .Build()
                    .Done();
            } else {
                if (range.From().Maybe<TKqlKeyInc>()) {
                    tupleComparison = MakeLE(range.From().Arg(i), column, ctx, pos);
                } else {
                    tupleComparison = MakeLT(range.From().Arg(i), column, ctx, pos);
                }
            }
        }

        if (tupleComparison.IsValid()) {
            conds.push_back(tupleComparison.Cast());
        }
    }

    {
        TMaybeNode<TExprBase> tupleComparison;
        for (ssize_t i = static_cast<ssize_t>(range.To().ArgCount()) - 1; i >= static_cast<ssize_t>(prefix); --i) {
            auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[i]).Done();
            if (tupleComparison.IsValid()) {
                tupleComparison = Build<TCoOr>(ctx, pos)
                    .Add(MakeLT(column, range.To().Arg(i), ctx, pos))
                    .Add<TCoAnd>()
                        .Add(MakeEQ(column, range.To().Arg(i), ctx, pos))
                        .Add(tupleComparison.Cast())
                        .Build()
                    .Done();
            } else {
                if (range.To().Maybe<TKqlKeyInc>()) {
                    tupleComparison = MakeLE(column, range.To().Arg(i), ctx, pos);
                } else {
                    tupleComparison = MakeLT(column, range.To().Arg(i), ctx, pos);
                }
            }
        }

        if (tupleComparison.IsValid()) {
            conds.push_back(tupleComparison.Cast());
        }
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({arg})
        .Body<TCoOptionalIf>()
            .Predicate<TCoCoalesce>()
                .Predicate<TCoAnd>()
                    .Add(conds)
                    .Build()
                .Value<TCoBool>()
                    .Literal().Build("false")
                    .Build()
                .Build()
            .Value(arg)
            .Build()
        .Done();
}

bool ExtractUsedFields(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TString>& usedFields, const TParentsMap& parentsMap, bool allowDependsOn) {
    const TTypeAnnotationNode* argType = RemoveOptionalType(arg.GetTypeAnn());
    if (argType->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }

    if (&arg == start.Get()) {
        return true;
    }

    const auto inputStructType = argType->Cast<TStructExprType>();
    if (!IsDepended(*start, arg)) {
        return true;
    }

    TNodeSet nodes;
    VisitExpr(start, [&](const TExprNode::TPtr& node) {
        nodes.insert(node.Get());
        return true;
    });

    const auto parents = parentsMap.find(&arg);
    YQL_ENSURE(parents != parentsMap.cend());
    for (const auto& parent : parents->second) {
        if (nodes.cend() == nodes.find(parent)) {
            continue;
        }

        if (parent->IsCallable("Member")) {
            usedFields.emplace(parent->Tail().Content());
        } else if (allowDependsOn && parent->IsCallable("DependsOn")) {
            continue;
        } else {
            // unknown node
            for (auto&& item : inputStructType->GetItems()) {
                usedFields.emplace(item->GetName());
            }
            return true;
        }
    }

    return true;
}

TExprBase TKqpMatchReadResult::BuildProcessNodes(TExprBase input, TExprContext& ctx) const {
    auto expr = input;

    if (ExtractMembers) {
        expr = Build<TCoExtractMembers>(ctx, ExtractMembers.Cast().Pos())
            .Input(expr)
            .Members(ExtractMembers.Cast().Members())
            .Done();
    }

    if (FilterNullMembers) {
        expr = Build<TCoFilterNullMembers>(ctx, FilterNullMembers.Cast().Pos())
            .Input(expr)
            .Members(FilterNullMembers.Cast().Members())
            .Done();
    }

    if (SkipNullMembers) {
        expr = Build<TCoSkipNullMembers>(ctx, SkipNullMembers.Cast().Pos())
            .Input(expr)
            .Members(SkipNullMembers.Cast().Members())
            .Done();
    }

    if (FlatMap) {
        expr = Build<TCoFlatMap>(ctx, FlatMap.Cast().Pos())
            .Input(expr)
            .Lambda(FlatMap.Cast().Lambda())
            .Done();
    }

    return expr;
}

TMaybe<TKqpMatchReadResult> MatchRead(TExprBase node, std::function<bool(TExprBase)> matchFunc) {
    auto expr = node;

    TMaybeNode<TCoFlatMap> flatmap;
    if (auto maybeNode = expr.Maybe<TCoFlatMap>()) {
        flatmap = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoSkipNullMembers> skipNullMembers;
    if (auto maybeNode = expr.Maybe<TCoSkipNullMembers>()) {
        skipNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoFilterNullMembers> filterNullMembers;
    if (auto maybeNode = expr.Maybe<TCoFilterNullMembers>()) {
        filterNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoExtractMembers> extractMembers;
    if (auto maybeNode = expr.Maybe<TCoExtractMembers>()) {
        extractMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    if (!matchFunc(expr)) {
        return {};
    }

    return TKqpMatchReadResult {
        .Read = expr,
        .ExtractMembers = extractMembers,
        .FilterNullMembers = filterNullMembers,
        .SkipNullMembers = skipNullMembers,
        .FlatMap = flatmap
    };
}

} // namespace NKikimr::NKqp::NOpt
