"""Generate reckon-go's C4 diagrams into assets/.

Run:  python3 architecture/c4_diagrams.py

  c4-context.svg     Level 1: who uses the SDK and the CLI, and what they reach
  c4-components.svg  Level 3: what the module is made of

Every box is a package or a file that exists on this branch, and every arrow an
import the Go compiler sees: the root facade builds each sub-client over one
grpc.ClientConn, each sub-client speaks only through genproto/gatewayv1, and the
CLI goes through the facade. When a package changes, change this file and re-run
it: a diagram that still names a package the module lost is drift.

Colours are plain classes with a prefers-color-scheme: dark block, no CSS custom
properties (rsvg-convert renders those black). Render and look after a change:
rsvg-convert -w 1400 assets/c4-context.svg -o /tmp/c4.png
"""
import os
from xml.sax.saxutils import escape

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "assets")

SANS = '"Atkinson Hyperlegible", "Segoe UI", system-ui, -apple-system, Helvetica, Arial, sans-serif'
MONO = '"JetBrains Mono", ui-monospace, Menlo, Consolas, monospace'

KINDS = {
    "b-sys": ("#F5E9CC", "#A56F0A", "#3A2E13", "#E3AA3C"),
    "b-per": ("#E3EBF2", "#3B5B7A", "#1D2A36", "#8EB4D8"),
    "b-mac": ("#E6E4EE", "#5E5A78", "#26243A", "#A9A4C8"),
    "b-ext": ("#ECEFED", "#8A9791", "#1C2421", "#7D8B85"),
    "b-cmp": ("#FFFFFF", "#8A9791", "#171F1C", "#7D8B85"),
}

STYLE = f"""<style>
.t-name {{ font-family: {SANS}; font-size: 15px; font-weight: 700; }}
.t-desc {{ font-family: {SANS}; font-size: 12.5px; }}
.t-rel {{ font-family: {SANS}; font-size: 11.5px; paint-order: stroke; stroke-width: 4px; stroke-linejoin: round; }}
.t-code {{ font-family: {MONO}; font-size: 13px; font-weight: 600; }}
.t-type {{ font-family: {MONO}; font-size: 11.5px; }}
.t-bound {{ font-family: {MONO}; font-size: 12px; font-weight: 600; }}
.t-leg {{ font-family: {SANS}; font-size: 12px; }}
.edge {{ fill: none; stroke-width: 1.25; }}
.edge-later {{ fill: none; stroke-width: 1.25; stroke-dasharray: 5 4; }}
.bg {{ fill: #FFFFFF; }}
""" + "".join(f".{k} {{ fill: {v[0]}; stroke: {v[1]}; stroke-width: {2 if k == 'b-sys' else 1.25}; }}\n" for k, v in KINDS.items()) + """\
.b-later { fill: none; stroke: #8A9791; stroke-width: 1.25; stroke-dasharray: 5 4; }
.bound { fill: none; stroke: #B3BDB8; stroke-width: 1; }
.head { fill: #3B5B7A; }
.t-name, .t-code { fill: #16201D; }
.t-type, .t-bound, .t-leg { fill: #66736E; }
.t-desc { fill: #34413C; }
.t-rel { fill: #66736E; stroke: #FFFFFF; }
.edge, .edge-later { stroke: #8A9791; }
.arrowhead { fill: #8A9791; }
@media (prefers-color-scheme: dark) {
.bg { fill: #171F1C; }
""" + "".join(f".{k} {{ fill: {v[2]}; stroke: {v[3]}; }}\n" for k, v in KINDS.items()) + """\
.b-later { stroke: #7D8B85; }
.bound { stroke: #4A5752; }
.head { fill: #8EB4D8; }
.t-name, .t-code { fill: #E6ECE9; }
.t-type, .t-bound, .t-leg { fill: #93A09A; }
.t-desc { fill: #C3CDC8; }
.t-rel { fill: #93A09A; stroke: #171F1C; }
.edge, .edge-later { stroke: #7D8B85; }
.arrowhead { fill: #7D8B85; }
}
</style>"""

DEFS = ('<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        'orient="auto-start-reverse"><path class="arrowhead" d="M0 0L10 5L0 10z"/></marker></defs>')

LEGEND = [("b-sys", "In scope"), ("b-per", "Person"), ("b-mac", "Platform it runs on"),
          ("b-ext", "External system"), ("b-cmp", "Component")]
BOUNDARY = ("bound", "Boundary")


def height_for(desc):
    return 74 + 16 * len(desc)


class Diagram:
    def __init__(self, name, title, w, h):
        self.name, self.title, self.w, self.h, self.parts = name, title, w, h, []

    def text(self, cls, x, y, s, anchor=None, rotate=None):
        a = f' text-anchor="{anchor}"' if anchor else ""
        r = f' transform="rotate({rotate} {x} {y})"' if rotate is not None else ""
        self.parts.append(f'<text class="{cls}" x="{x}" y="{y}"{a}{r}>{escape(s)}</text>')

    def box(self, cls, x, y, w, name, kind, desc, person=False, code=False):
        h = height_for(desc)
        self.parts.append(f'<rect class="{cls}" x="{x}" y="{y}" width="{w}" height="{h}" rx="10"/>')
        tx = x + 16
        if person:
            self.parts.append(f'<circle class="head" cx="{x + 26}" cy="{y + 24}" r="9"/>')
            tx = x + 44
        self.text("t-code" if code else "t-name", tx, y + 28, name)
        self.text("t-type", tx, y + 46, kind)
        for i, line in enumerate(desc):
            self.text("t-desc", x + 16, y + 70 + 16 * i, line)
        return h

    def bound(self, x, y, w, h, label):
        self.parts.append(f'<rect class="bound" x="{x}" y="{y}" width="{w}" height="{h}" rx="12"/>')
        self.text("t-bound", x + 16, y + 22, label)

    def edge(self, d, later=False):
        self.parts.append(f'<path class="{"edge-later" if later else "edge"}" d="{d}" marker-end="url(#a)"/>')

    def line(self, d):
        self.parts.append(f'<path class="edge" d="{d}"/>')

    def legend(self, y, boundary=True):
        x = 24
        for cls, label in LEGEND + ([BOUNDARY] if boundary else []):
            self.parts.append(f'<rect class="{cls}" x="{x}" y="{y}" width="22" height="14" rx="3"/>')
            self.text("t-leg", x + 30, y + 12, label)
            x += 40 + 6.9 * len(label)

    def write(self):
        svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" role="img" '
               f'aria-label="{escape(self.title)}">', f"<title>{escape(self.title)}</title>", STYLE, DEFS,
               f'<rect class="bg" x="0" y="0" width="{self.w}" height="{self.h}"/>'] + self.parts + ["</svg>", ""]
        with open(os.path.join(OUT, self.name), "w") as f:
            f.write("\n".join(svg))


def context():
    d = Diagram("c4-context.svg", "reckon-go system context", 1000, 720)
    d.box("b-per", 30, 30, 250, "Go application developer", "[Person]",
          ["Imports the module and calls", "one sub-client per service,", "with TLS on by default"], person=True)
    d.box("b-per", 375, 30, 250, "Shell or CI user", "[Person]",
          ["Runs the reckon binary: JSON", "out, NDJSON for streams, and", "an exit code per failure"], person=True)
    d.box("b-per", 720, 30, 250, "Editor integration", "[Person: reckon-nvim]",
          ["Drives the binary and reads", "its JSON, so the editor never", "speaks gRPC itself"], person=True)
    d.box("b-ext", 30, 210, 250, "Go application", "[External: the caller's program]",
          ["Links the SDK in process:", "no sidecar and no daemon,", "one Client per endpoint"])
    d.box("b-sys", 375, 210, 250, "reckon-go", "[In scope: one Go module, v0.10.0]",
          ["An SDK with nine service", "sub-clients behind one facade,",
           "and the reckon CLI that exposes", "the same surface as JSON for", "shells and editors"])
    d.box("b-ext", 720, 210, 250, "reckon-gateway", "[External: gRPC endpoint]",
          ["Answers reckon.gateway.v1:", "Stores, Stream, Subscription,",
           "Snapshot, Schema, Temporal,", "Admin, Health and DCB. The SDK",
           "has no default address"])
    d.box("b-ext", 30, 430, 250, "reckon-proto", "[External: the wire contract]",
          ["Proto package reckon.gateway.v1;", "the stubs in genproto are",
           "generated from it and committed,", "never generated at build time"])
    d.box("b-ext", 375, 430, 250, "System CA trust store", "[External: the OS]",
          ["TLS is verified against the system", "roots unless Insecure, TLSWithCA",
           "or TLSWithServerName says otherwise"])
    d.edge("M155 157V210"); d.text("t-rel", 163, 188, "writes it")
    d.edge("M280 285H375"); d.text("t-rel", 327, 268, "imports", anchor="middle"); d.text("t-rel", 327, 305, "the module", anchor="middle")
    d.edge("M500 157V210"); d.text("t-rel", 508, 202, "runs the binary")
    d.edge("M845 157V172H500V210"); d.text("t-rel", 700, 164, "drives the binary", anchor="middle")
    d.edge("M625 285H720"); d.text("t-rel", 672, 268, "gRPC over", anchor="middle"); d.text("t-rel", 672, 305, "HTTP/2", anchor="middle")
    d.edge("M155 430V400H420V364"); d.text("t-rel", 163, 420, "generated into genproto")
    d.edge("M500 364V430"); d.text("t-rel", 508, 400, "verifies against")
    d.text("t-desc", 30, 600, "The SDK has no default endpoint: Connect needs an explicit host:port, and only the CLI and the examples")
    d.text("t-desc", 30, 616, "supply localhost:50051. There is no application authentication anywhere in the module: a caller that")
    d.text("t-desc", 30, 632, "needs a token or a client certificate passes its own grpc.DialOption, or wraps the exposed connection.")
    d.legend(686, boundary=False)
    d.write()


def components():
    d = Diagram("c4-components.svg", "reckon-go components", 1000, 1050)
    d.bound(16, 40, 968, 794, "reckon-go module [Container: a Go library and the reckon CLI]")
    d.box("b-cmp", 32, 70, 452, "client.go: the facade", "[reckon.Client, one grpc.ClientConn]",
          ["Connect rewrites a bare host:port to", "passthrough:/// so no DNS SRV lookup",
           "runs; Insecure, TLSWithCA and", "TLSWithServerName set the transport."], code=True)
    d.box("b-cmp", 516, 70, 452, "cmd/reckon: the CLI", "[route.go, cmd_*.go, encode/]",
          ["One group per service, JSON out and", "NDJSON for streams; global flags for",
           "endpoint, store, timeout, bytes and", "transport, and an exit code per failure."], code=True)
    d.box("b-cmp", 32, 268, 210, "streams", "[plus lineage]",
          ["Append with optimistic", "concurrency, forward and",
           "backward reads, indexed", "reads, and cause and",
           "correlation traversal"], code=True)
    d.box("b-cmp", 274, 268, 210, "subscriptions", "[specs, delivery, ack]",
          ["Persistent specs, stream", "and pattern filters, a",
           "delivery channel, and", "checkpoint, lag and ack"], code=True)
    d.box("b-cmp", 516, 268, 210, "snapshots", "[Spec, Record]",
          ["Save, read at a version,", "latest, delete and list,",
           "per source, stream and", "version"], code=True)
    d.box("b-cmp", 758, 268, 210, "schema", "[Definition]",
          ["Register, unregister,", "get and list event",
           "schemas, with versions", "and upcasting"], code=True)
    d.box("b-cmp", 32, 482, 210, "temporal", "[ReadUntil, ReadRange]",
          ["Wall-clock and", "time-travel reads that", "answer streams records"], code=True)
    d.box("b-cmp", 274, 482, 210, "admin", "[stats, scavenge, links]",
          ["Store stats, stream", "info, scavenge (dry run",
           "and matching), projection", "links, catalogue reload"], code=True)
    d.box("b-cmp", 516, 482, 210, "health", "[checks and diagnostics]",
          ["Store check, cluster", "consistency, membership,",
           "Raft log, memory level", "and server info"], code=True)
    d.box("b-cmp", 758, 482, 210, "stores", "[topology discovery]",
          ["List, get and watch the", "gateway's stores and",
           "their topology; the only", "store-free sub-client"], code=True)
    d.box("b-cmp", 32, 680, 210, "dcb", "[dcb.go, ccc.go]",
          ["Append if no tag matches,", "read a DCB context, and",
           "the CCC payload reads", "that need store indexes"], code=True)
    d.box("b-cmp", 274, 680, 452, "genproto/gatewayv1: the wire contract", "[generated by buf from reckon-proto]",
          ["Stubs for the nine services of proto package reckon.gateway.v1. Committed, never",
           "regenerated at build time, and the only thing any sub-client talks to."], code=True)
    d.box("b-ext", 32, 880, 304, "reckon-gateway", "[External endpoint]",
          ["Answers the nine services", "over gRPC, TLS by default"], None)
    d.edge("M258 208V268"); d.text("t-rel", 266, 244, "builds")
    d.edge("M500 208V268"); d.text("t-rel", 508, 244, "builds")
    d.edge("M742 208V268"); d.text("t-rel", 750, 244, "builds")
    d.edge("M863 208V268"); d.text("t-rel", 871, 244, "builds")
    d.edge("M516 120H484"); d.text("t-rel", 480, 112, "imports", anchor="end")
    d.line("M137 620V650H863V620")
    d.line("M379 620V650"); d.line("M621 620V650")
    d.edge("M500 650V680"); d.text("t-rel", 508, 672, "through the stubs")
    d.edge("M242 750H274"); d.text("t-rel", 258, 742, "uses", anchor="middle")
    d.edge("M500 786V880"); d.text("t-rel", 508, 840, "grpc-go over TLS")
    d.legend(1010)
    d.write()

if __name__ == "__main__":
    for make in (context, components):
        make()
