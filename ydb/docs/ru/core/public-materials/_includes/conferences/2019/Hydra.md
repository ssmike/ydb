## Распределенные транзакции в YDB {#2019-conf-hydra}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

В докладе рассмотрен алгоритм планирования транзакций, на котором основана транзакционная система {{ ydb-short-name }}. Вы узнаете, какие сущности участвуют в транзакциях, кто устанавливает глобальный порядок транзакций и как достигается атомарность транзакций, надежность и изоляция высокого уровня.

@[YouTube](https://www.youtube.com/watch?v=85GIFpG3zx4)

На примере общей проблемы показана реализация транзакций с использованием двухфазной фиксации и детерминированных транзакций.

[Слайды](https://presentations.ydb.tech/2019/ru/hydra/presentation.pdf)