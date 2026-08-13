"""ONLINE FEATURE STORE (Redis).

Layout key (dinh nghia trong config/features/feature_spec.yml):

    fs:{version}:u:{user_id}   HASH   36 selected feature batch cua 1 user
    rt:u:{user_id}             HASH   overlay realtime (TTL ngan), ghi sau raw lake
    fs:meta:active_version     STRING con tro toi version dang phuc vu  <-- ATOMIC SWAP
    fs:meta:{version}:status   HASH   trang thai + so lieu cua lan sync
    fs:meta:{version}:shards   SET    shard da ghi xong (idempotency marker)
    fs:meta:versions           ZSET   version -> epoch (dung de GC)

Vi sao co `active_version` thay vi ghi de truc tiep len key dang dung:
  - Ghi de truc tiep = trong luc sync, mot phan user dung feature moi, mot
    phan dung feature cu -> serving khong nhat quan.
  - Ghi vao namespace version moi roi SET 1 key con tro = doi toan bo traffic
    sang phien ban moi trong 1 lenh atomic. Loi thi chi can tro nguoc lai
    (rollback tuc thi, khong can ghi lai du lieu).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from lzd_pipeline.common.clients import get_redis
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.spec import FeatureSpec, load_feature_spec

log = get_logger(__name__)

# ===========================================================================
# CUA SO TRUOT cho feature realtime
#
# Counter rt_* phai co nghia la "trong 1 gio VUA ROI". Neu chi HINCRBY vao 1
# field roi EXPIRE lai key sau moi lan ghi thi voi user hoat dong lien tuc,
# key KHONG BAO GIO het han -> counter cong don vo han. Luc do offline (dbt
# tinh dung 1h) va online (cong don ca ngay) lech nhau hoan toan.
#
# Cach lam: chia gio thanh 12 o 5 phut. Moi o la mot field rieng trong hash,
# ten dang "<feature>|<bucket_start_epoch>". Doc = cong cac o con nam trong
# cua so. Ghi = cong vao o hien tai + don cac o da rot ra ngoai.
#
# dbt/models/marts/feat_user_realtime_pit.sql dung DUNG cong thuc cua so nay.
# ===========================================================================
RT_WINDOW_SECONDS = 3600
RT_BUCKET_SECONDS = 300
RT_BUCKETS = RT_WINDOW_SECONDS // RT_BUCKET_SECONDS      # 12
RT_FIELD_SEP = "|"


def rt_bucket_start(now: float) -> int:
    """Moc bat dau cua o 5 phut chua thoi diem `now`."""
    return int(now // RT_BUCKET_SECONDS) * RT_BUCKET_SECONDS


def rt_cutoff(now: float) -> int:
    """O cu nhat con duoc tinh vao cua so."""
    return rt_bucket_start(now) - (RT_BUCKETS - 1) * RT_BUCKET_SECONDS


# --- Lua: doc active_version + 2 hash trong MOT round-trip, ATOMIC ---------
# Vi sao can Lua: neu lam 2 buoc rieng (GET active_version roi moi HGETALL)
# thi (a) ton 2 RTT, (b) giua 2 buoc do version co the bi doi/GC -> request
# doc trung namespace vua bi xoa -> cache miss gia. Lua chay nguyen khoi tren
# Redis nen khong co khe ho nao chen vao giua.
#
# KEYS[1] = fs:meta:active_version   KEYS[2] = rt:u:<user_id>
# ARGV[1..3] = 3 manh cua template key batch (truoc version / giua / sau)
_LUA_READ_MERGED = """
local version = redis.call('GET', KEYS[1])
if not version then
  return {'', {}, redis.call('HGETALL', KEYS[2])}
end
local batch_key = ARGV[1] .. version .. ARGV[2] .. ARGV[3]
return {version,
        redis.call('HGETALL', batch_key),
        redis.call('HGETALL', KEYS[2])}
"""

# --- Lua: cong counter vao o hien tai + don o het han, ATOMIC --------------
# KEYS[1] = rt:u:<user_id>
# ARGV[1] = bucket_start   ARGV[2] = cutoff   ARGV[3] = ttl
# ARGV[4] = so cap         ARGV[5..] = field1, delta1, field2, delta2, ...
_LUA_INCR_WINDOW = """
local bucket = ARGV[1]
local cutoff = tonumber(ARGV[2])
local ttl    = tonumber(ARGV[3])
local pairs_n = tonumber(ARGV[4])

local i = 5
for _ = 1, pairs_n do
  redis.call('HINCRBYFLOAT', KEYS[1], ARGV[i] .. '|' .. bucket, ARGV[i + 1])
  i = i + 2
end

-- Don cac o da rot khoi cua so (neu khong hash se phinh vo han)
local fields = redis.call('HKEYS', KEYS[1])
for _, f in ipairs(fields) do
  local sep = string.find(f, '|', 1, true)
  if sep then
    local b = tonumber(string.sub(f, sep + 1))
    if b and b < cutoff then
      redis.call('HDEL', KEYS[1], f)
    end
  end
end

redis.call('EXPIRE', KEYS[1], ttl)
return 1
"""


def _decode(value: Any) -> str:
    """Lua tra ve bytes ngay ca khi client dat decode_responses=True."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


def _flat_to_dict(flat: Any) -> dict[str, str]:
    """HGETALL trong Lua tra ve mang phang [k1, v1, k2, v2, ...]."""
    if not flat:
        return {}
    if isinstance(flat, dict):          # duong du phong
        return flat
    items = [_decode(x) for x in flat]
    return dict(zip(items[0::2], items[1::2]))


@dataclass
class WriteResult:
    rows_written: int
    duration_ms: int
    skipped: bool = False   # True neu shard da DONE tu lan chay truoc


class OnlineFeatureStore:
    """Bao boc moi thao tac Redis cua feature store."""

    def __init__(self, redis_client=None, spec: FeatureSpec | None = None) -> None:
        self.r = redis_client or get_redis()
        self.spec = spec or load_feature_spec()
        self.cfg = get_settings().feature_store
        self._lua_ok = True          # tat neu Redis khong ho tro EVAL
        self._script_read = None
        self._script_incr = None

    # ------------------------------------------------------------------
    _NO_LUA = object()   # sentinel: Redis khong chay duoc Lua

    def _run_script(self, attr: str, source: str, keys: list, args: list) -> Any:
        """Chay Lua script; tra ve _NO_LUA neu Redis khong ho tro.

        Bat loi ca luc dang ky LAN luc thuc thi: fakeredis (unit test) dang ky
        duoc nhung khong chay duoc neu thieu `lupa`. Gap loi thi tat han va
        chuyen sang duong du phong, khong thu lai moi request.
        """
        if not self._lua_ok:
            return self._NO_LUA
        try:
            script = getattr(self, attr)
            if script is None:
                script = self.r.register_script(source)
                setattr(self, attr, script)
            return script(keys=keys, args=args)
        except Exception as exc:
            self._lua_ok = False
            log.warning(
                "Redis khong chay duoc Lua - dung duong du phong "
                "(them round-trip, khong atomic)",
                extra={"event": "lua_unavailable", "error": str(exc)[:200]},
            )
            return self._NO_LUA

    # ==================================================================
    # META / VERSION
    # ==================================================================
    @property
    def _active_key(self) -> str:
        return self.spec.meta_key("active_version")

    def get_active_version(self) -> str | None:
        return self.r.get(self._active_key)

    def activate_version(self, version: str) -> str | None:
        """Doi toan bo traffic sang version moi. Tra ve version cu (de rollback)."""
        previous = self.r.get(self._active_key)
        pipe = self.r.pipeline()
        pipe.set(self._active_key, version)
        pipe.hset(self.spec.meta_key(version, "status"),
                  mapping={"status": "ACTIVE", "activated_at": int(time.time())})
        # Active version khong duoc co TTL - neu truoc do bi set stale TTL thi go ra
        pipe.persist(self.spec.meta_key(version, "status"))
        pipe.zadd(self.spec.meta_key("versions"), {version: time.time()})
        pipe.execute()
        log.info("kich hoat feature version",
                 extra={"event": "version_activated", "feature_version": version,
                        "previous_version": previous})
        return previous

    def rollback_to(self, version: str) -> None:
        self.r.set(self._active_key, version)
        log.warning("rollback feature version",
                    extra={"event": "version_rollback", "feature_version": version})

    def set_version_status(self, version: str, **fields: Any) -> None:
        payload = {k: ("" if v is None else str(v)) for k, v in fields.items()}
        payload.setdefault("updated_at", str(int(time.time())))
        self.r.hset(self.spec.meta_key(version, "status"), mapping=payload)
        self.r.zadd(self.spec.meta_key("versions"), {version: time.time()}, nx=True)

    def get_version_status(self, version: str) -> dict[str, str]:
        return self.r.hgetall(self.spec.meta_key(version, "status")) or {}

    def list_versions(self) -> list[str]:
        """Version theo thu tu moi -> cu."""
        return self.r.zrevrange(self.spec.meta_key("versions"), 0, -1) or []

    def feature_store_age_seconds(self) -> float | None:
        version = self.get_active_version()
        if not version:
            return None
        status = self.get_version_status(version)
        ts = status.get("activated_at") or status.get("updated_at")
        return time.time() - float(ts) if ts else None

    # ==================================================================
    # GHI (batch sync)
    # ==================================================================
    def is_shard_done(self, version: str, shard_id: int) -> bool:
        return bool(self.r.sismember(self.spec.meta_key(version, "shards"), str(shard_id)))

    def mark_shard_done(self, version: str, shard_id: int, rows: int) -> None:
        pipe = self.r.pipeline()
        pipe.sadd(self.spec.meta_key(version, "shards"), str(shard_id))
        pipe.hincrby(self.spec.meta_key(version, "status"), "rows_written", rows)
        pipe.execute()

    def write_rows(
        self,
        version: str,
        rows: Iterable[dict[str, Any]],
        batch_size: int | None = None,
    ) -> int:
        """Ghi nhieu user vao namespace `version` bang pipeline.

        Moi row phai co khoa `user_id` + cac cot feature batch.
        Dung HSET (ghi de toan bo field) -> chay lai cung du lieu cho ket qua
        y het lan dau => IDEMPOTENT o muc row.
        """
        batch_size = batch_size or self.cfg.batch_size
        field_names = self.spec.batch_names
        written = 0
        pipe = self.r.pipeline(transaction=False)
        pending = 0

        for row in rows:
            user_id = row.get(self.spec.entity_key)
            if user_id is None:
                continue
            mapping = {}
            for name in field_names:
                value = row.get(name)
                if value is None:
                    continue                       # thieu -> de API dung default
                mapping[name] = value
            if not mapping:
                continue
            mapping["_v"] = version                # de debug tren redis-cli
            mapping["_ts"] = int(time.time())
            if feature_set_id := self.spec.offline.get("selected_feature_set_id"):
                mapping["_feature_set_id"] = feature_set_id
            pipe.hset(self.spec.batch_key(version, str(user_id)), mapping=mapping)
            pending += 1
            written += 1
            if pending >= batch_size:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
                pending = 0

        if pending:
            pipe.execute()
        return written

    def write_shard(
        self,
        version: str,
        shard_id: int,
        rows: Iterable[dict[str, Any]],
        skip_if_done: bool = True,
    ) -> WriteResult:
        """Ghi 1 shard, bo qua neu shard da DONE (resume sau khi loi giua chung)."""
        if skip_if_done and self.is_shard_done(version, shard_id):
            log.info("bo qua shard da xong",
                     extra={"event": "shard_skipped", "feature_version": version,
                            "shard_id": shard_id})
            return WriteResult(rows_written=0, duration_ms=0, skipped=True)

        started = time.perf_counter()
        written = self.write_rows(version, rows)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        self.mark_shard_done(version, shard_id, written)
        log.info("ghi shard xong",
                 extra={"event": "shard_written", "feature_version": version,
                        "shard_id": shard_id, "rows": written, "duration_ms": elapsed_ms})
        return WriteResult(rows_written=written, duration_ms=elapsed_ms)

    # ==================================================================
    # GHI (realtime overlay tu Kafka)
    # ==================================================================
    def update_realtime(self, user_id: str, values: dict[str, Any], ttl: int | None = None) -> None:
        key = self.spec.realtime_key(user_id)
        pipe = self.r.pipeline()
        pipe.hset(key, mapping={k: v for k, v in values.items() if v is not None})
        pipe.expire(key, ttl or self.cfg.realtime_ttl_seconds)
        pipe.execute()

    def update_realtime_bulk(
        self, updates: dict[str, dict[str, Any]], ttl: int | None = None
    ) -> int:
        """Cap nhat overlay cho nhieu user trong 1 pipeline (dung boi consumer)."""
        ttl = ttl or self.cfg.realtime_ttl_seconds
        pipe = self.r.pipeline(transaction=False)
        for user_id, values in updates.items():
            key = self.spec.realtime_key(user_id)
            clean = {k: v for k, v in values.items() if v is not None}
            if not clean:
                continue
            pipe.hset(key, mapping=clean)
            pipe.expire(key, ttl)
        pipe.execute()
        return len(updates)

    def incr_realtime_counters(
        self,
        user_id: str,
        counters: dict[str, float],
        ttl: int | None = None,
        now: float | None = None,
    ) -> None:
        """Cong counter realtime vao O 5 PHUT hien tai (cua so truot 1 gio).

        LOI CU (da sua): chi HINCRBY vao mot field roi EXPIRE lai key sau moi
        lan ghi. Voi user hoat dong lien tuc, TTL bi day lui mai mai nen key
        khong bao gio het han va rt_events_1h cong don ca ngay. Trong khi do
        dbt tinh dung cua so 1 gio -> luc train model thay 12, luc serve thay
        400. Day CHINH LA training/serving skew, chi khac la no nam o nhanh
        realtime chu khong phai nhanh batch.
        """
        now = now or time.time()
        bucket = rt_bucket_start(now)
        cutoff = rt_cutoff(now)
        key = self.spec.realtime_key(user_id)
        ttl = ttl or self.cfg.realtime_ttl_seconds

        args: list[Any] = [bucket, cutoff, ttl, len(counters)]
        for field, delta in counters.items():
            args.extend([field, delta])
        if self._run_script("_script_incr", _LUA_INCR_WINDOW,
                            keys=[key], args=args) is not self._NO_LUA:
            return

        # Duong du phong (Redis khong co Lua): cung ket qua, khong atomic
        pipe = self.r.pipeline()
        for field, delta in counters.items():
            pipe.hincrbyfloat(key, f"{field}{RT_FIELD_SEP}{bucket}", float(delta))
        pipe.expire(key, ttl)
        pipe.execute()
        for field in self.r.hkeys(key):
            if RT_FIELD_SEP in field:
                _, _, suffix = field.partition(RT_FIELD_SEP)
                if suffix.isdigit() and int(suffix) < cutoff:
                    self.r.hdel(key, field)

    def aggregate_realtime(
        self, rt_raw: dict[str, Any], now: float | None = None
    ) -> dict[str, Any]:
        """Gap hash o-5-phut thanh gia tri cua so 1 gio.

        Field co dang "<feature>|<bucket_start>" duoc CONG lai neu o do con
        nam trong cua so. Field khong co dau '|' (vd rt_last_event_ts) la gia
        tri tuyet doi -> giu nguyen, ghi de.
        """
        if not rt_raw:
            return {}
        cutoff = rt_cutoff(now or time.time())
        out: dict[str, Any] = {}
        for field, value in rt_raw.items():
            if RT_FIELD_SEP not in field:
                out[field] = value
                continue
            name, _, suffix = field.partition(RT_FIELD_SEP)
            try:
                if int(suffix) < cutoff:
                    continue                      # o da rot khoi cua so
                out[name] = float(out.get(name, 0.0)) + float(value)
            except (TypeError, ValueError):
                continue
        return out

    # ==================================================================
    # DOC (serving path) - phai < 10ms
    # ==================================================================
    def get_features(
        self, user_id: str, version: str | None = None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Doc hash batch + hash realtime khi DA BIET version.

        Dung cho validate/DQ - nhung cho can chi dinh version cu the.
        Duong serving dung `read_for_serving()` de khoi phai doc
        active_version bang mot round-trip rieng.
        """
        version = version or self.get_active_version()
        if not version:
            return {}, {}
        pipe = self.r.pipeline(transaction=False)
        pipe.hgetall(self.spec.batch_key(version, user_id))
        pipe.hgetall(self.spec.realtime_key(user_id))
        batch_raw, rt_raw = pipe.execute()
        return batch_raw or {}, rt_raw or {}

    def read_for_serving(self, user_id: str) -> tuple[str | None, dict, dict]:
        """DUONG SERVING: 1 round-trip, atomic.

        Tra ve (active_version, hash batch, hash realtime).

        Truoc day duong nay lam 2 buoc:
            GET fs:meta:active_version      <- RTT 1
            HGETALL fs:{v}:u:{uid} + rt:... <- RTT 2
        Vua gap doi do tre Redis, vua co khe ho: giua 2 buoc, DAG sync co the
        activate version moi roi GC version cu -> request doc trung namespace
        vua bi xoa va bao cache miss oan. Lua chay nguyen khoi nen khong con
        khe ho do.
        """
        template = self.spec.online["batch_key_template"]
        head, rest = template.split("{version}", 1)
        mid, tail = rest.split("{user_id}", 1)

        result = self._run_script(
            "_script_read",
            _LUA_READ_MERGED,
            keys=[self._active_key, self.spec.realtime_key(user_id)],
            args=[head, mid + user_id, tail],
        )
        if result is self._NO_LUA:
            version = self.get_active_version()
            batch_raw, rt_raw = self.get_features(user_id, version=version)
            return version, batch_raw, rt_raw

        version, batch_flat, rt_flat = result
        return (
            _decode(version) or None,
            _flat_to_dict(batch_flat),
            _flat_to_dict(rt_flat),
        )

    def get_features_merged(
        self, user_id: str, version: str | None = None, now: float | None = None
    ) -> tuple[dict[str, Any], int, bool]:
        """Tra ve (feature day du da cast, so feature thieu, cache_hit)."""
        if version is None:
            version, batch_raw, rt_raw = self.read_for_serving(user_id)
        else:
            batch_raw, rt_raw = self.get_features(user_id, version)
        merged, missing = self.spec.merge(
            batch_raw, self.aggregate_realtime(rt_raw, now=now)
        )
        return merged, missing, bool(batch_raw)

    def mget_features(
        self, user_ids: Sequence[str], version: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Batch lookup cho nhieu user (dung cho scoring hang loat).

        Doc active_version MOT lan roi dung cho ca lo -> toan bo lo cung mot
        phien ban feature, khong co chuyen nua lo v1 nua lo v2.
        """
        version = version or self.get_active_version()
        if not version:
            return {uid: {} for uid in user_ids}
        now = time.time()
        pipe = self.r.pipeline(transaction=False)
        for uid in user_ids:
            pipe.hgetall(self.spec.batch_key(version, uid))
            pipe.hgetall(self.spec.realtime_key(uid))
        raw = pipe.execute()
        out: dict[str, dict[str, Any]] = {}
        for idx, uid in enumerate(user_ids):
            batch_raw = raw[idx * 2] or {}
            rt_raw = raw[idx * 2 + 1] or {}
            merged, _ = self.spec.merge(
                batch_raw, self.aggregate_realtime(rt_raw, now=now)
            )
            out[uid] = merged
        return out

    # ==================================================================
    # CACHE INVALIDATION / GC
    # ==================================================================
    def expire_version(self, version: str, ttl: int | None = None) -> int:
        """Ha TTL cho toan bo key cua 1 version (xoa mem, khong block Redis).

        Dung SCAN + EXPIRE thay vi KEYS/DEL: KEYS block toan bo server,
        cam tuyet doi tren production.
        """
        ttl = ttl or self.cfg.stale_ttl_seconds
        pattern = self.spec.batch_key(version, "*")
        touched = 0
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                pipe = self.r.pipeline(transaction=False)
                for key in keys:
                    pipe.expire(key, ttl)
                pipe.execute()
                touched += len(keys)
            if cursor == 0:
                break
        self.set_version_status(version, status="RETIRED", retired_at=int(time.time()))
        log.info("dat TTL cho version cu",
                 extra={"event": "version_expired", "feature_version": version,
                        "keys": touched, "ttl": ttl})
        return touched

    def delete_version(self, version: str) -> int:
        """Xoa han 1 version (UNLINK = xoa bat dong bo, khong block)."""
        pattern = self.spec.batch_key(version, "*")
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                self.r.unlink(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        pipe = self.r.pipeline()
        pipe.unlink(self.spec.meta_key(version, "status"))
        pipe.unlink(self.spec.meta_key(version, "shards"))
        pipe.zrem(self.spec.meta_key("versions"), version)
        pipe.execute()
        log.info("xoa version",
                 extra={"event": "version_deleted", "feature_version": version, "keys": deleted})
        return deleted

    def gc_old_versions(self, keep: int | None = None) -> dict[str, Any]:
        """Giu `keep` version moi nhat (luon giu active), xoa phan con lai."""
        keep = keep or self.cfg.versions_to_keep
        active = self.get_active_version()
        versions = self.list_versions()
        keep_set = set(versions[:keep])
        if active:
            keep_set.add(active)
        removed = []
        for version in versions:
            if version in keep_set:
                continue
            self.delete_version(version)
            removed.append(version)
        return {"kept": sorted(keep_set), "removed": removed, "active": active}

    # ==================================================================
    # TIEN ICH
    # ==================================================================
    def count_keys(self, pattern: str) -> int:
        """Dem key theo pattern bang SCAN (an toan, khong block)."""
        cursor = 0
        total = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=1000)
            total += len(keys)
            if cursor == 0:
                return total

    def sample_user_ids(self, version: str, limit: int = 100) -> list[str]:
        """Lay ngau nhien vai user_id cua 1 version - dung de validate."""
        pattern = self.spec.batch_key(version, "*")
        prefix_len = len(self.spec.batch_key(version, ""))
        out: list[str] = []
        cursor = 0
        while len(out) < limit:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(limit, 200))
            out.extend(k[prefix_len:] for k in keys)
            if cursor == 0:
                break
        return out[:limit]
