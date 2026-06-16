import json, subprocess, sys
WH="5d3bf93a93859cf8"
def run(sql):
    payload=json.dumps({"warehouse_id":WH,"statement":sql,"wait_timeout":"50s","format":"JSON_ARRAY"})
    out=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--json",payload],capture_output=True,text=True).stdout
    d=json.loads(out)
    st=d.get("status",{}).get("state")
    sid=d.get("statement_id")
    while st in ("PENDING","RUNNING"):
        out=subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}"],capture_output=True,text=True).stdout
        d=json.loads(out); st=d.get("status",{}).get("state")
    if st!="SUCCEEDED":
        print("STATE:",st, d.get("status")); return None,None
    sch=d.get("manifest",{}).get("schema",{})
    cols=[c["name"] for c in sch.get("columns",[])]
    rows=d.get("result",{}).get("data_array",[]) or []
    return cols,rows
if __name__=="__main__":
    cols,rows=run(sys.argv[1])
    if cols is None: sys.exit(1)
    print("\t".join(cols))
    for r in rows: print("\t".join("" if v is None else str(v) for v in r))
