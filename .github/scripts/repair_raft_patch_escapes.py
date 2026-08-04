from pathlib import Path

path = Path(".github/scripts/apply_raft_hardening.py")
text = path.read_text()
replacements = {
    "b'\\n'": "b'\\\\n'",
    'b"\\n"': 'b"\\\\n"',
    'b"not-json\\n"': 'b"not-json\\\\n"',
}
for old, new in replacements.items():
    count = text.count(old)
    if count == 0:
        raise SystemExit(f"expected at least one patch-script escape target: {old!r}")
    text = text.replace(old, new)
path.write_text(text)
