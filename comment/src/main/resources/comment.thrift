namespace py ss.comment.dedup
namespace java ss.comment.dedup

struct Comment {
  1: i64 id,
  2: string content,
  3: i64 create_time,
}

service dedup {
  map<i64, i64> dedup(1: list<Comment> comments)
}