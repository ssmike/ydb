# Generated by devtools/yamaker.

PROGRAM(clang-tidy-confusable-chars-gen)

VERSION(16.0.0)

LICENSE(
    Apache-2.0 WITH LLVM-exception AND
    Unicode
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/llvm16
    contrib/libs/llvm16/lib/Support
)

ADDINCL(
    contrib/libs/clang16/tools/extra/clang-tidy/misc/ConfusableTable
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    BuildConfusableTable.cpp
)

END()
