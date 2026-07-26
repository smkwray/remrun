"""Render the mesh reachability matrix for a terminal.

Deliberately a matrix and not an ASCII node-graph: SSH trust is directed, and a
drawn graph with ~7 nodes and up to 42 directed edges is unreadable, whereas a
grid makes asymmetry obvious — you read across for "who can this box reach" and
down for "who can reach this box".
"""
from __future__ import annotations

from .mesh import AUTH, GLYPH, OK, SELF, UNKNOWN

LEGEND = [
    ("Y", "can ssh in"),
    ("!", "reachable, key not authorized"),
    ("x", "connection refused (sshd off)"),
    (".", "offline / no route"),
    ("?", "hostname did not resolve"),
    ("k", "host key not trusted"),
    ("-", "self"),
    (" ", "not tested (hop unreachable)"),
]


def render_matrix(matrix: dict, controller: str) -> str:
    rows = matrix["rows"]
    edges = matrix["edges"]
    width = max(len(r) for r in rows)

    lines = ["Rows = FROM (source), columns = TO (target).", ""]
    header = " " * (width + 2) + " ".join(name[:4].ljust(4) for name in rows)
    lines.append(header)

    for source in rows:
        cells = []
        for target in rows:
            edge = edges.get(source, {}).get(target)
            status = edge.status if edge else UNKNOWN
            if source == target:
                status = SELF
            cells.append(GLYPH.get(status, " ").ljust(4))
        label = (source + ("*" if source == controller else "")).ljust(width + 2)
        lines.append(label + " ".join(cells).rstrip())

    lines.append("")
    lines.append("  ".join(f"{glyph} {text}" for glyph, text in LEGEND[:3]))
    lines.append("  ".join(f"{glyph} {text}" for glyph, text in LEGEND[3:6]))
    lines.append("  ".join(f"{glyph} {text}" for glyph, text in LEGEND[6:]))
    lines.append("")
    lines.append(f"* this controller ({controller})")

    problems = _asymmetries(matrix)
    if problems:
        lines.append("")
        lines.append("Asymmetric trust (one direction works, the other does not):")
        lines.extend(f"  {line}" for line in problems)

    unreachable = _unreachable_by_anyone(matrix)
    if unreachable:
        lines.append("")
        lines.append("No node can ssh into these (a key must be installed with "
                     "physical/console access):")
        lines.extend(f"  {name}" for name in unreachable)
    return "\n".join(lines)


def _asymmetries(matrix: dict) -> list[str]:
    rows, edges = matrix["rows"], matrix["edges"]
    seen, out = set(), []
    for a in rows:
        for b in rows:
            if a == b or (b, a) in seen:
                continue
            forward = edges.get(a, {}).get(b)
            back = edges.get(b, {}).get(a)
            # Only report when BOTH directions were actually measured.
            if not forward or not back:
                continue
            if (forward.status == OK) != (back.status == OK):
                seen.add((a, b))
                good, bad = (a, b) if forward.status == OK else (b, a)
                reason = (back if forward.status == OK else forward).detail
                out.append(f"{good} -> {bad} works, but {bad} -> {good} does not"
                           + (f" ({reason})" if reason else ""))
    return out


def _unreachable_by_anyone(matrix: dict) -> list[str]:
    """Targets no measured source could log into — the chicken-and-egg cases."""
    rows, edges = matrix["rows"], matrix["edges"]
    out = []
    for target in rows:
        attempts = [edges[s][target] for s in rows
                    if s != target and target in edges.get(s, {})]
        # Only interesting if something actually ANSWERED and refused: a box that
        # is merely powered off is not a trust problem.
        if (attempts and not any(e.status == OK for e in attempts)
                and any(e.status == AUTH for e in attempts)):
            out.append(target)
    return out


def to_dict(matrix: dict) -> dict:
    return {
        "rows": matrix["rows"],
        "edges": {
            source: {target: {"status": edge.status, "detail": edge.detail}
                     for target, edge in targets.items()}
            for source, targets in matrix["edges"].items()
        },
    }
