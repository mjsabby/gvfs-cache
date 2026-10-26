"""More busy keep-alive clients than workers: N clients send GETs back to back for DUR s against
opus with --workers W; prints answers per client and when each got its first. (Written for the
thread-per-request edge; on tokio, W is the runtime's thread count and every client is a task.)

usage: busy_conns.py <opus-binary> <workers> <clients>   (env DUR; SearchGold store sg/opus2)
"""
import http.client, os, sys, threading, time
import impls
binary, workers, nconn = sys.argv[1], sys.argv[2], int(sys.argv[3])
DUR = float(os.environ.get("DUR", "4"))
SG = "/home/muks/gvfs-bench/sg"
im = impls.ALL["opus"](os.path.join(SG, "opus2"), binary=binary, extra_serve=["--workers", workers])
srv = im.serve(os.path.join(SG, "up.git"), live=False, ready_timeout=900)
oids = [l.strip() for l in open(os.path.join(SG, "oids", "blobs.txt")) if l.strip()][:5000]
counts = [0] * nconn
errs = [0] * nconn
first = [None] * nconn
start = threading.Barrier(nconn + 1)
def run(i):
    c = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=DUR + 5)
    start.wait()
    t0 = time.time()
    k = i
    while time.time() - t0 < DUR:
        try:
            c.request("GET", f"/gvfs/objects/{oids[k % len(oids)]}")
            r = c.getresponse(); r.read()
            if r.status == 200:
                counts[i] += 1
                if first[i] is None:
                    first[i] = time.time() - t0
        except Exception:
            errs[i] += 1
            break
        k += nconn
ts = [threading.Thread(target=run, args=(i,)) for i in range(nconn)]
for t in ts: t.start()
start.wait()
for t in ts: t.join()
srv.stop()
print(f"workers={workers} conns={nconn}: per-connection completions {counts}")
print("  first response after (s):", [None if f is None else round(f, 3) for f in first], "errors", errs)
print("  clients first answered after half the run (starved):",
      sum(1 for f in first if f is None or f > DUR / 2))
