namespace py com.dedup.thrift.comment
namespace java com.dedup.thrift.comment

struct Req {
  1: i64 id,
  2: string content,
  3: i64 create_time,
}

service Dedup {
  map<i64, i64> dedup(1: list<Req> reqs)
}