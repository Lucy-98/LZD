-- ===========================================================================
-- BIZ SCHEMA — Feature-consistent Event Reconstruction (Track A) + handoff
--
-- docs/RECONSTRUCTION_SPEC.md §9 (provenance) · §10 (immutable target)
--
-- NGUYEN TAC: schema nay KHONG duoc lam thay doi semantics cua `Provenance`
-- da pass 139 test. No CHI them lop luu tru. P-3/P-4 duoc lap lai thanh CHECK
-- constraint => hai lop (dataclass + DB) cung ma hoa MOT luat.
-- ===========================================================================
\connect pipeline

CREATE SCHEMA IF NOT EXISTS biz;

-- ---------------------------------------------------------------------------
-- 1) generation_run — §11 reproducibility fingerprint
--    Cung fingerprint => cung event_hash, cung reconstructed_feature_hash,
--    cung ket qua gate (Gate G).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS biz.generation_run (
    generation_run_id         TEXT        PRIMARY KEY,
    root_generation_id        TEXT        NOT NULL,
    parent_run_id             TEXT        REFERENCES biz.generation_run(generation_run_id),

    -- Moi thanh phan cua fingerprint (§11) — thieu mot cai la Gate G mat hieu luc
    selected_feature_set_id   TEXT        NOT NULL,
    feature_spec_version      TEXT        NOT NULL,
    constraint_model_version  TEXT        NOT NULL,
    encoding_version          TEXT        NOT NULL,
    objective_version         TEXT        NOT NULL,   -- objective LA semantics
    selection_policy_version  TEXT        NOT NULL,   -- seed KHONG mo ta du policy
    solver_version            TEXT        NOT NULL,
    generation_seed           BIGINT      NOT NULL,
    reference_ts              TIMESTAMPTZ NOT NULL,   -- KHONG BAO GIO now()

    -- Epistemic metadata — §10.1
    semantic_branch           TEXT        CHECK (semantic_branch IN ('H1','H2')),
    semantic_status           TEXT        NOT NULL
                              CHECK (semantic_status IN
                                     ('H1_SUPPORTED','H2_SUPPORTED','UNIDENTIFIED')),

    reproducibility_fingerprint TEXT      NOT NULL,
    track                     TEXT        NOT NULL CHECK (track IN ('A','B')),
    started_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at               TIMESTAMPTZ,

    -- I-6: status SUPPORTED ep dung branch (lap lai rang buoc cua SolverConfig)
    CONSTRAINT ck_status_branch CHECK (
        (semantic_status = 'UNIDENTIFIED')
     OR (semantic_status = 'H1_SUPPORTED' AND semantic_branch = 'H1')
     OR (semantic_status = 'H2_SUPPORTED' AND semantic_branch = 'H2')
    ),
    -- P-1 (mot phan): khong tu tro chinh minh. Chu trinh dai hon kiem o §5.
    CONSTRAINT ck_no_self_parent CHECK (parent_run_id IS DISTINCT FROM generation_run_id)
);
CREATE INDEX IF NOT EXISTS idx_gen_run_root ON biz.generation_run (root_generation_id);
CREATE INDEX IF NOT EXISTS idx_gen_run_fp   ON biz.generation_run (reproducibility_fingerprint);

-- ---------------------------------------------------------------------------
-- 2) reconstruction_target - §10 IMMUTABLE
--    🚫 Khong component nao duoc phep sua target de validation pass.
-- ---------------------------------------------------------------------------
-- Ham phu cho ck_scope (Postgres khong co san). Phai ton tai TRUOC table.
CREATE OR REPLACE FUNCTION biz.jsonb_object_keys_count(j JSONB) RETURNS INT
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT count(*)::INT FROM jsonb_object_keys(j) $$;

CREATE TABLE IF NOT EXISTS biz.reconstruction_target (
    target_id                 TEXT        PRIMARY KEY,
    lzd_user_id               TEXT        NOT NULL,
    selected_feature_set_id   TEXT        NOT NULL,
    feature_version           TEXT        NOT NULL,
    reference_ts              TIMESTAMPTZ NOT NULL,
    split                     TEXT        NOT NULL CHECK (split = 'train'),
    payload                   JSONB       NOT NULL,   -- DUNG 30 cot (fs_2026_08_v4)
    target_hash               TEXT        NOT NULL,   -- tamper-evidence §6.3
    feature_payload_hash      TEXT        NOT NULL,   -- §6.1a
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- I-1 / Gate E: label & is_treat KHONG BAO GIO duoc co mat
    CONSTRAINT ck_no_label CHECK (NOT (payload ? 'label' OR payload ? 'is_treat')),
    -- INVARIANT 1: dung 30 cot, khong hon.
    -- Con so nay phai khop `expected_column_count` cua fs_2026_08_v4.yaml.
    -- Doi scope => doi CA HAI, va bump `selected_feature_set_id`.
    CONSTRAINT ck_scope_30 CHECK (biz.jsonb_object_keys_count(payload) = 30)
);

-- 🚫 Chan UPDATE/DELETE — immutable la rang buoc, khong phai loi hua
CREATE OR REPLACE FUNCTION biz.forbid_target_mutation() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'biz.reconstruction_target la IMMUTABLE (SPEC §10). '
        'Khong duoc sua target de validation pass.';
END $$;

DROP TRIGGER IF EXISTS trg_target_immutable ON biz.reconstruction_target;
CREATE TRIGGER trg_target_immutable
    BEFORE UPDATE OR DELETE ON biz.reconstruction_target
    FOR EACH ROW EXECUTE FUNCTION biz.forbid_target_mutation();

-- Decoded T2 attributes and immutable T3 payload used by the forward engine.
CREATE TABLE IF NOT EXISTS biz.customer_attribute (
    target_id                 TEXT        NOT NULL REFERENCES biz.reconstruction_target(target_id),
    attr_name                 TEXT        NOT NULL,
    level_id                  INT         NOT NULL CHECK (level_id >= 0),
    PRIMARY KEY (target_id, attr_name)
);

CREATE TABLE IF NOT EXISTS biz.encoding_map (
    encoding_version          TEXT        NOT NULL,
    attr_name                 TEXT        NOT NULL,
    level_id                  INT         NOT NULL CHECK (level_id >= 0),
    column_name               TEXT        NOT NULL,
    value                     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (encoding_version, attr_name, level_id, column_name)
);

CREATE TABLE IF NOT EXISTS biz.onehot_layout (
    encoding_version          TEXT        NOT NULL,
    attr_name                 TEXT        NOT NULL,
    level_index               INT         NOT NULL CHECK (level_index >= 0),
    column_name               TEXT        NOT NULL,
    PRIMARY KEY (encoding_version, attr_name, level_index)
);

-- 21 cot T3 cua fs_2026_08_v4. Phai khop `tiers["T3"]` cua artifact —
-- neu thieu cot, feat_passthrough.sql se select mot cot khong ton tai.
CREATE TABLE IF NOT EXISTS biz.passthrough_source (
    target_id                 TEXT        PRIMARY KEY REFERENCES biz.reconstruction_target(target_id),
    f0  DOUBLE PRECISION, f3  DOUBLE PRECISION, f4  DOUBLE PRECISION,
    f6  DOUBLE PRECISION, f7  DOUBLE PRECISION, f8  DOUBLE PRECISION,
    f9  DOUBLE PRECISION, f10 DOUBLE PRECISION, f13 DOUBLE PRECISION,
    f16 DOUBLE PRECISION, f17 DOUBLE PRECISION, f20 DOUBLE PRECISION,
    f21 DOUBLE PRECISION, f22 DOUBLE PRECISION, f23 DOUBLE PRECISION,
    f25 DOUBLE PRECISION, f26 DOUBLE PRECISION, f27 DOUBLE PRECISION,
    f28 DOUBLE PRECISION, f29 DOUBLE PRECISION, f35 DOUBLE PRECISION
);

-- ---------------------------------------------------------------------------
-- 3) provenance — §9.1, dung chung cho moi ban ghi sinh ra
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS biz.provenance (
    provenance_id             BIGSERIAL   PRIMARY KEY,
    source_type               TEXT        NOT NULL
                              CHECK (source_type IN ('REAL','RECONSTRUCTED','SYNTHETIC')),
    generation_run_id         TEXT        NOT NULL REFERENCES biz.generation_run(generation_run_id),
    root_generation_id        TEXT        NOT NULL,
    parent_run_id             TEXT,
    parent_target_id          TEXT        REFERENCES biz.reconstruction_target(target_id),
    parent_entity_id          TEXT,
    parent_feature_version    TEXT,
    ancestor_model_ids        TEXT[]      NOT NULL DEFAULT '{}',
    behaviour_policy_version  TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- P-3: RECONSTRUCTED phai truy duoc ve target
    CONSTRAINT ck_p3 CHECK (
        source_type <> 'RECONSTRUCTED' OR parent_target_id IS NOT NULL
    ),
    -- P-4: SYNTHETIC phai truy vet duoc CO CHE SINH — ML hoac rule deu hop le
    CONSTRAINT ck_p4 CHECK (
        source_type <> 'SYNTHETIC'
     OR cardinality(ancestor_model_ids) > 0
     OR behaviour_policy_version IS NOT NULL
    )
);
CREATE INDEX IF NOT EXISTS idx_prov_root ON biz.provenance (root_generation_id);
CREATE INDEX IF NOT EXISTS idx_prov_run  ON biz.provenance (generation_run_id);

-- ---------------------------------------------------------------------------
-- 4) reconstruction_result + event witness
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS biz.reconstruction_result (
    result_id                 BIGSERIAL   PRIMARY KEY,
    target_id                 TEXT        NOT NULL REFERENCES biz.reconstruction_target(target_id),
    generation_run_id         TEXT        NOT NULL REFERENCES biz.generation_run(generation_run_id),
    status                    TEXT        NOT NULL
                              CHECK (status IN ('SOLVED','REPAIRED','QUARANTINED')),
    reason_code               TEXT,
    repair_rounds             INT         NOT NULL DEFAULT 0,
    pool_size                 INT,
    -- §3.2: objective_summary BAT BUOC khi SOLVED/REPAIRED
    objective_value           INT[],
    unexplained_events        INT,
    active_days               INT,
    sessions                  INT,
    total_events              INT,
    reconstructed_feature_hash TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_objective_required CHECK (
        status = 'QUARANTINED' OR objective_value IS NOT NULL
    ),
    CONSTRAINT ck_quarantine_no_events CHECK (
        status <> 'QUARANTINED' OR total_events IS NULL OR total_events = 0
    ),
    UNIQUE (target_id, generation_run_id)
);

CREATE TABLE IF NOT EXISTS biz.reconstruction_event (
    event_id                  TEXT        PRIMARY KEY,
    result_id                 BIGINT      NOT NULL REFERENCES biz.reconstruction_result(result_id) ON DELETE CASCADE,
    gen_reason                TEXT        NOT NULL,
    day_offset                INT         NOT NULL CHECK (day_offset >= 0),
    sub_index                 INT         NOT NULL CHECK (sub_index >= 0),
    occurrence                INT         NOT NULL CHECK (occurrence >= 0),
    event_ts                  TIMESTAMPTZ NOT NULL,
    event_type                TEXT        NOT NULL,
    -- candidate_slot_id §4.3a-1 phai duy nhat trong mot result
    UNIQUE (result_id, gen_reason, day_offset, sub_index)
);
CREATE INDEX IF NOT EXISTS idx_recon_event_result ON biz.reconstruction_event (result_id);

-- ---------------------------------------------------------------------------
-- 5) customer_state — T0 DA XAC NHAN, dau vao DUY NHAT cua Track B (§3.4)
--    🚫 KHONG co cot nao chua gia tri feature.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS biz.customer_state (
    state_id                  BIGSERIAL   PRIMARY KEY,
    customer_id               TEXT        NOT NULL,
    as_of_ts                  TIMESTAMPTZ NOT NULL,
    source_target_id          TEXT        NOT NULL,   -- OPAQUE lineage id (§3.4-1)
    attributes                JSONB       NOT NULL,   -- attr -> level_id
    counters                  JSONB       NOT NULL,   -- counter da giai ma
    last_event_ts             TIMESTAMPTZ,
    semantic_branch           TEXT        CHECK (semantic_branch IN ('H1','H2')),
    semantic_status           TEXT        NOT NULL,
    provenance_id             BIGINT      NOT NULL REFERENCES biz.provenance(provenance_id),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Gate B §12.1: moi event Track A nam TRUOC as_of_ts
    CONSTRAINT ck_last_event_before CHECK (
        last_event_ts IS NULL OR last_event_ts < as_of_ts
    ),
    UNIQUE (customer_id, as_of_ts, source_target_id)
);

-- 🚫 KHONG co FK tu customer_state -> reconstruction_target.
--    Do la CO Y: FK se bien source_target_id thanh lookup key va vi pham
--    INVARIANT 4. Rang buoc referential duoc giu o provenance.parent_target_id,
--    thu ma Track B khong doc.

-- ---------------------------------------------------------------------------
-- 6) quarantine + diff — §13 constraint model
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS biz.reject_target (
    target_id                 TEXT        NOT NULL,
    generation_run_id         TEXT        NOT NULL REFERENCES biz.generation_run(generation_run_id),
    reason_code               TEXT        NOT NULL,
    detail                    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (target_id, generation_run_id)
);

CREATE TABLE IF NOT EXISTS biz.reconstruction_diff (
    id                        BIGSERIAL   PRIMARY KEY,
    target_id                 TEXT        NOT NULL,
    generation_run_id         TEXT        NOT NULL,
    column_name               TEXT        NOT NULL,
    regime                    TEXT        NOT NULL,
    expected                  DOUBLE PRECISION,
    actual                    DOUBLE PRECISION,
    rel_error                 DOUBLE PRECISION,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 6b) ★ PIT BOUNDARY VIEW — thu DUY NHAT feature engine duoc doc tu target
--
-- Feature engine CAN `reference_ts` de biet bien point-in-time, giong het
-- `feat_user_realtime_pit` doc `feature_ts` tu snapshot.
-- Nhung no TUYET DOI khong duoc thay `payload` (30 gia tri feature) — neu thay,
-- no co the "doc dap an" va Gate A thanh vo nghia (§12 tautology).
--
-- View nay phoi bay DUNG ba cot. dbt source tro vao DAY, khong tro vao bang goc.
-- ---------------------------------------------------------------------------
-- ⚠️ Ten cot PHAI la `customer_id_hint`, khong phai `lzd_user_id`.
--    `feat_cfs_counter.sql` / `feat_cfs_recency.sql` doc view nay qua
--    `source('biz','reconstruction_boundary')` va select `customer_id_hint`.
--    Lech ten => model do o PRODUCTION voi "column does not exist", trong khi
--    harness van xanh vi harness seed bang bang phang cua rieng no.
--
--    Hau to `_hint` la co y: day KHONG phai khoa join uy quyen: Track B chi
--    duoc mang ID de truy vet, khong duoc dung no de doc lai target (§3.4-1).
CREATE OR REPLACE VIEW biz.v_reconstruction_boundary AS
SELECT target_id, lzd_user_id AS customer_id_hint, reference_ts
FROM biz.reconstruction_target;

COMMENT ON VIEW biz.v_reconstruction_boundary IS
    'PIT boundary cho forward feature engine. CHI target_id + lzd_user_id + '
    'reference_ts. 🚫 KHONG BAO GIO them cot payload vao day — do la duong '
    'ro ri bien Gate A thanh tautology (SPEC §12, TA-3).';

-- ---------------------------------------------------------------------------
-- 7) Lineage — P-1 (khong chu trinh) va P-5 (kiem tren DO THI)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW biz.v_run_lineage AS
WITH RECURSIVE walk(generation_run_id, ancestor_id, depth, path, is_cycle) AS (
    SELECT r.generation_run_id, r.parent_run_id, 1,
           ARRAY[r.generation_run_id], FALSE
    FROM biz.generation_run r
  UNION ALL
    SELECT w.generation_run_id, r.parent_run_id, w.depth + 1,
           w.path || r.generation_run_id,
           r.generation_run_id = ANY(w.path)
    FROM walk w
    JOIN biz.generation_run r ON r.generation_run_id = w.ancestor_id
    WHERE NOT w.is_cycle AND w.depth < 64
)
SELECT * FROM walk;

-- P-1: bat ky dong nao co is_cycle = TRUE la vi pham
CREATE OR REPLACE VIEW biz.v_lineage_cycles AS
SELECT DISTINCT generation_run_id, path
FROM biz.v_run_lineage WHERE is_cycle;

-- P-5: chi lineage thuan REAL moi duoc vao tap train production
CREATE OR REPLACE VIEW biz.v_real_lineage_roots AS
SELECT root_generation_id
FROM biz.provenance
GROUP BY root_generation_id
HAVING bool_and(source_type = 'REAL')
   AND bool_and(cardinality(ancestor_model_ids) = 0);

COMMENT ON VIEW biz.v_real_lineage_roots IS
    'P-5: root_generation_id du dieu kien cho tap train production. '
    'Kiem tren DO THI, khong tren tung dong — mot dong REAL van co the co '
    'to tien SYNTHETIC qua nhieu doi.';

GRANT ALL ON SCHEMA biz TO CURRENT_USER;
