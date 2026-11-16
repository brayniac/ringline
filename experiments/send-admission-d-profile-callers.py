import collections,gzip,os,re,sys,tarfile
"""Count the callers of one leaf symbol in send-admission-d-profile.toml runs.

Usage: send-admission-d-profile-callers.py <leaf> <depth> <dir>...
Prints, per run, the five most common chains of <depth> callers above
samples whose leaf is <leaf>, in microseconds of CPU 0 per reply.
"""
leaf=sys.argv[1]; depth=int(sys.argv[2])
for d in sys.argv[3:]:
    tgz=[os.path.join(r,"results.tgz") for r,_,f in os.walk(d) if "results.tgz" in f][0]
    name=[f for f in os.listdir(d) if f.startswith("D profile")][0]
    raw=gzip.decompress(tarfile.open(tgz).extractfile("results/perf-script.txt.gz").read()).decode(errors="replace")
    c=collections.Counter()
    for b in raw.split("\n\n"):
        fr=re.findall(r"^\s+[0-9a-f]+\s+(\S+)",b,re.M)
        if fr and fr[0]==leaf: c[" < ".join(fr[1:1+depth])]+=1
    print("==",name)
    for k,v in c.most_common(5): print(f"  {v*250/1010:6.0f} us/reply  {k}")
