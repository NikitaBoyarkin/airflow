"""DQ-валидация данных перед записью во внешний приёмник (REQ-003).

`validate()` возвращает список нарушений; пустой список == данные валидны.
`validate_or_raise()` — удобная обёртка для DAG-тасков: нет данных ->
NoDataError, прочие нарушения -> ValueError.
"""

from __future__ import annotations

from collections.abc import Sequence

from shared.alerts import NoDataError

DEFAULT_HEADER = ("sale_date", "product_id", "revenue", "orders")


def validate(
    data: Sequence[Sequence[str]], *, header: Sequence[str] = DEFAULT_HEADER
) -> list[str]:
    """Проверки: непустой набор, совпадение заголовка, числовые поля >= 0.

    data — строки как из `hook.get_records()`: первая строка = явный заголовок
    (DAG добавляет header перед выгрузкой). Поля `revenue`/`orders` ищутся по
    имени в header — валидатор не завязан на конкретную схему. Возвращает
    список нарушений.
    """
    issues: list[str] = []
    if not data:
        return ["empty dataset: нет ни одной строки"]

    rows = [list(r) for r in data]
    if rows[0] != list(header):
        issues.append(
            f"header mismatch: ожидалось {list(header)!r}, получено {rows[0]!r}"
        )

    body = rows[1:]
    if not body:
        issues.append("no data rows: только заголовок, данных за период нет")

    rev_idx: int | None
    ord_idx: int | None
    try:
        rev_idx = list(header).index("revenue")
        ord_idx = list(header).index("orders")
    except ValueError:
        rev_idx = ord_idx = None

    for excel_row, row in enumerate(body, start=2):
        if len(row) != len(header):
            issues.append(
                f"row {excel_row}: ожидалось {len(header)} колонок, получено {len(row)}"
            )
            continue
        if not row[0].strip():
            issues.append(f"row {excel_row}: пустой {header[0]}")
        if rev_idx is not None:
            revenue = row[rev_idx]
            try:
                if float(revenue) < 0:
                    issues.append(f"row {excel_row}: revenue < 0 ({revenue!r})")
            except (TypeError, ValueError):
                issues.append(f"row {excel_row}: revenue не число ({revenue!r})")
        if ord_idx is not None:
            orders = row[ord_idx]
            try:
                if int(orders) < 0:
                    issues.append(f"row {excel_row}: orders < 0 ({orders!r})")
            except (TypeError, ValueError):
                issues.append(f"row {excel_row}: orders не число ({orders!r})")

    return issues


def validate_or_raise(
    data: Sequence[Sequence[str]], *, header: Sequence[str] = DEFAULT_HEADER
) -> list[list[str]]:
    """DQ-гейт для DAG-тасков: валидно -> данные как есть, иначе исключение.

    Нет данных (только заголовок / пустой набор) -> NoDataError — штатный
    исход, о котором оператор должен узнать. Прочие нарушения -> ValueError.
    """
    issues = validate(data, header=header)
    if not issues:
        return data  # type: ignore[return-value]  # тот же объект, как в исходном DAG
    if all("no data" in issue or "empty" in issue for issue in issues):
        raise NoDataError("no data rows for the requested window")
    raise ValueError("DQ violations:\n" + "\n".join(issues))
