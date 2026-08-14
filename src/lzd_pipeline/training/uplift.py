"""DR-Learner + thuoc do uplift — port tu notebook `lzd-uplifting-model`.

    03_model_tuning.ipynb      fit_nuisance · dr_from_nuisance · DRLearner
    04_benchmark_evaluation.ipynb  qini_curve · gain_curve · qini_score ·
                                   auuc_score · uplift_at_k

★ VI SAO PORT CHU KHONG VIET LAI
--------------------------------------------------------------------------
Model dang phuc vu (`models/uplift_voucher/model_booster.txt`) sinh ra tu
chinh doan code nay. Viet lai mot phien ban "tuong duong" se lam hai thu troi
khoi nhau ma khong ai biet — huan luyen lai trong repo se cho model KHAC
model dang chay, trong khi ca hai deu tu goi la DRLearner.

Cong thuc, hang so va ca cach pha the hoa deu giu NGUYEN VAN. Cho nao doi so
voi notebook thi ghi ro ly do ngay tai cho.

★ DR-LEARNER LA GI (Kennedy 2020)
--------------------------------------------------------------------------
Khong hoi quy Y truc tiep, ma hoi quy mot **pseudo-outcome doubly robust**:

    dr = (mu1 - mu0)  +  W*(Y - mu1)/e  -  (1-W)*(Y - mu0)/(1-e)

    e(X)   = P(W=1|X)        propensity
    mu_w(X)= E[Y|X, W=w]     outcome theo tung nhanh

"Doubly robust" = uoc luong van khong chech neu **mot trong hai** (e hoac mu)
dung. Ba mo hinh phu deu uoc luong bang **cross-fitting** de phan du khong
tuong quan voi chinh mau da fit — bo cross-fitting di la overfit ngay.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import numpy as np

#: Seed cua notebook. Doi no => doi ca fold split lan cach pha the hoa.
SEED = 42

#: `[MEASURED]` Nguong trim propensity, suy tu quy tac Crump et al. 2009 tren
#: chinh du lieu (alpha tho = 0.0708), roi kep vao hang rao [0.02, 0.10].
#: Doc tu `artifacts/best_params.json: xu_ly_overlap.propensity_alpha`.
#:
#: 🚫 KHONG phai so tuy chon. `best_params.json` ghi ro: *"Notebook 04 phai doc
#:    propensity_alpha o day va dung dung nguong do, neu khong thi bo sieu tham
#:    so tinh chinh cho mo hinh nay lai ap cho mo hinh khac."*
E_ALPHA = 0.071
E_CLIP: tuple[float, float] = (E_ALPHA, 1.0 - E_ALPHA)

#: Sieu tham so cua ba mo hinh phu. Co dinh trong notebook — KHONG duoc tune,
#: vi tune nuisance theo cung metric se lam ro ri thong tin vao pseudo-outcome.
NUISANCE_PARAMS: dict[str, Any] = {
    "n_estimators": 200,
    "learning_rate": 0.05,
    "max_depth": 5,
    "min_child_samples": 50,
    "random_state": SEED,
    "verbose": -1,
}

#: `[MEASURED]` Sieu tham so cua DR-Learner, do Optuna tim (12 trial, muc tieu
#: `dr_qini`). Trung khop `metadata.json: sieu_tham_so`.
BEST_PARAMS: dict[str, Any] = {
    "n_estimators": 350,
    "learning_rate": 0.01777174904859463,
    "max_depth": 4,
    "min_child_samples": 30,
}

_TRAPZ = getattr(np, "trapezoid", None) or np.trapz


# ===========================================================================
# Mo hinh phu — cross-fitting
# ===========================================================================
def fit_nuisance(
    X: np.ndarray, W: np.ndarray, Y: np.ndarray, *, n_folds: int = 5, seed: int = SEED
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Uoc luong `e`, `mu0`, `mu1` bang cross-fitting. Tra ve `e` THO, chua cat.

    Fold duoc phan tang theo `W*2 + Y` — bon to hop (control/treat) x
    (khong mua/mua). Phan tang theo rieng `W` se de mot fold gan nhu khong co
    ca mua, va `m1`/`m0` fit tren fold do se vo nghia.
    """
    import lightgbm as lgb
    from sklearn.model_selection import StratifiedKFold

    n = len(X)
    e_raw = np.zeros(n)
    mu0_hat = np.zeros(n)
    mu1_hat = np.zeros(n)

    strata = W * 2 + Y
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=seed)

    for tr, te in skf.split(X, strata):
        m_e = lgb.LGBMClassifier(**NUISANCE_PARAMS).fit(X[tr], W[tr])
        e_raw[te] = m_e.predict_proba(X[te])[:, 1]

        m0 = lgb.LGBMClassifier(**NUISANCE_PARAMS).fit(
            X[tr][W[tr] == 0], Y[tr][W[tr] == 0])
        m1 = lgb.LGBMClassifier(**NUISANCE_PARAMS).fit(
            X[tr][W[tr] == 1], Y[tr][W[tr] == 1])
        mu0_hat[te] = m0.predict_proba(X[te])[:, 1]
        mu1_hat[te] = m1.predict_proba(X[te])[:, 1]

    return e_raw, mu0_hat, mu1_hat


def dr_from_nuisance(
    W: np.ndarray, Y: np.ndarray,
    e_raw: np.ndarray, mu0_hat: np.ndarray, mu1_hat: np.ndarray,
    clip: tuple[float, float] = E_CLIP,
) -> tuple[np.ndarray, np.ndarray]:
    """Ghep pseudo-outcome doubly robust, cat `e` theo `clip`.

    ⚠️ Cat la BAT BUOC, khong phai lam dep: mau so co `e` va `1-e`, nen `e`
    gan 0 hoac 1 lam pseudo-outcome no ra vo cung va nuot toan bo tin hieu.
    """
    e_hat = np.clip(e_raw, *clip)
    dr = (
        (mu1_hat - mu0_hat)
        + W * (Y - mu1_hat) / e_hat
        - (1 - W) * (Y - mu0_hat) / (1 - e_hat)
    )
    return dr, e_hat


def compute_dr_scores(
    X: np.ndarray, W: np.ndarray, Y: np.ndarray, *,
    n_folds: int = 5, seed: int = SEED, clip: tuple[float, float] | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """`(dr, e_hat, mu0, mu1)`. `clip=None` => dung `E_CLIP`."""
    clip = E_CLIP if clip is None else clip
    e_raw, mu0_hat, mu1_hat = fit_nuisance(X, W, Y, n_folds=n_folds, seed=seed)
    dr, e_hat = dr_from_nuisance(W, Y, e_raw, mu0_hat, mu1_hat, clip)
    return dr, e_hat, mu0_hat, mu1_hat


# ===========================================================================
# DR-Learner
# ===========================================================================
@dataclass
class DRLearner:
    """Kennedy 2020 — hoi quy pseudo-outcome doubly robust.

    Giu nguyen chu ky cua notebook: `fit(X, W, Y)` va `predict_cate(X)`.
    `predict_cate` la mot dong `self.model.predict(X)` — dung nhu ban da dong
    goi trong `model.pkl`, nen booster xuat ra tu day thay the duoc 1-1.
    """

    n_folds: int = 5
    params: dict[str, Any] = field(default_factory=dict)
    model: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.params = dict(self.params or BEST_PARAMS, random_state=SEED, verbose=-1)

    def fit(self, X: np.ndarray, W: np.ndarray, Y: np.ndarray) -> "DRLearner":
        import lightgbm as lgb

        dr_train, *_ = compute_dr_scores(X, W, Y, n_folds=self.n_folds)
        self.model = lgb.LGBMRegressor(**self.params).fit(X, dr_train)
        return self

    def predict_cate(self, X: np.ndarray) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("model chua fit()")
        return self.model.predict(X)

    def booster_text(self) -> str:
        """Xuat cay duoi dang text — dinh dang DOC LAP PHIEN BAN PYTHON.

        Day la thu duoc bundle vao `models/uplift_voucher/` va nap luc serving.
        🚫 Khong dung cloudpickle: no nhung code object, ma code object khong
           tuong thich giua cac ban Python (da do: nap ban 3.10 bang 3.14 thi
           do `TypeError: code() argument 13 must be str, not int`).
        """
        if self.model is None:
            raise RuntimeError("model chua fit()")
        return self.model.booster_.model_to_string()


# ===========================================================================
# Thuoc do
# ===========================================================================
def _sap_xep(y, w, score, seed: int = SEED):
    """Sap giam dan theo score, pha the hoa NGAU NHIEN CO SEED.

    ⚠️ Pha the hoa la load-bearing. Rat nhieu user co cung score (cay quyet
    dinh cho ra gia tri roi rac); neu de thu tu on dinh theo chi so dong thi
    Qini se an theo thu tu dong cua file — mot dai luong khong lien quan gi
    toi model.
    """
    n = len(y)
    rng = np.random.RandomState(seed)
    order = np.lexsort((rng.rand(n), -np.asarray(score, dtype=float)))
    y_, w_ = np.asarray(y)[order], np.asarray(w)[order]
    return (np.cumsum(w_), np.cumsum(1 - w_),
            np.cumsum(y_ * w_), np.cumsum(y_ * (1 - w_)))


def qini_curve(y, w, score, seed: int = SEED):
    """`(x = ti le dan so duoc nham, y = so don tang them tren moi nguoi)`."""
    nt, nc, yt, yc = _sap_xep(y, w, score, seed)
    n = len(y)
    ty_le = np.divide(nt, nc, out=np.zeros(n), where=nc > 0)
    return np.arange(1, n + 1) / n, (yt - yc * ty_le) / n


def gain_curve(y, w, score, seed: int = SEED):
    n = len(y)
    nt, nc, yt, yc = _sap_xep(y, w, score, seed)
    r_t = np.divide(yt, nt, out=np.zeros(n), where=nt > 0)
    r_c = np.divide(yc, nc, out=np.zeros(n), where=nc > 0)
    return np.arange(1, n + 1) / n, (r_t - r_c) * (nt + nc) / n


def oracle_score(y, w) -> np.ndarray:
    """Xep hang BIET TRUOC dap an — tran tren de chuan hoa, khong phai model."""
    return np.where(np.asarray(w) == 1, y, -np.asarray(y)).astype(float)


def _dien_tich_tren_duong_cheo(x, g) -> float:
    return float(_TRAPZ(g, x) - 0.5 * g[-1])


def qini_tho(y, w, score, seed: int = SEED) -> float:
    return _dien_tich_tren_duong_cheo(*qini_curve(y, w, score, seed))


def qini_score(y, w, score, seed: int = SEED) -> float:
    """He so Qini CHUAN HOA. Ngau nhien ~ 0, biet truoc = 1."""
    tran = qini_tho(y, w, oracle_score(y, w), seed)
    return qini_tho(y, w, score, seed) / tran if tran > 0 else float("nan")


def auuc_score(y, w, score, seed: int = SEED) -> float:
    """Nhu tren nhung tren duong Cumulative Gain — de doi chieu."""
    x, g = gain_curve(y, w, score, seed)
    xo, go = gain_curve(y, w, oracle_score(y, w), seed)
    tran = _dien_tich_tren_duong_cheo(xo, go)
    return _dien_tich_tren_duong_cheo(x, g) / tran if tran > 0 else float("nan")


def uplift_at_k(y, w, score, k: float = 0.2, seed: int = SEED) -> float:
    """Uplift THUC DO trong nhom k% co CATE du doan cao nhat."""
    n = len(y)
    rng = np.random.RandomState(seed)
    order = np.lexsort((rng.rand(n), -np.asarray(score, dtype=float)))
    top = order[:max(1, int(round(k * n)))]
    yt, wt = np.asarray(y)[top], np.asarray(w)[top]
    if wt.sum() == 0 or (1 - wt).sum() == 0:
        return float("nan")
    return float(yt[wt == 1].mean() - yt[wt == 0].mean())


def ate_with_ci(y, w, alpha: float = 0.05) -> dict[str, float]:
    """ATE, sai so chuan va khoang tin cay tren tap RCT."""
    from scipy import stats

    y = np.asarray(y, dtype=float)
    w = np.asarray(w)
    y1, y0 = y[w == 1], y[w == 0]
    p1, p0 = y1.mean(), y0.mean()
    se = float(np.sqrt(p1 * (1 - p1) / len(y1) + p0 * (1 - p0) / len(y0)))
    z = float(stats.norm.ppf(1 - alpha / 2))
    ate = float(p1 - p0)
    return {"ate": ate, "se": se, "ci_thap": ate - z * se, "ci_cao": ate + z * se,
            "n_treat": int(len(y1)), "n_control": int(len(y0))}


def evaluate_all(y, w, score, seed: int = SEED) -> dict[str, float]:
    """Bo metric day du — dung cho `train.evaluate()` va bao cao."""
    return {
        "qini": qini_score(y, w, score, seed),
        "auuc": auuc_score(y, w, score, seed),
        "uplift_at_10": uplift_at_k(y, w, score, 0.10, seed),
        "uplift_at_20": uplift_at_k(y, w, score, 0.20, seed),
        "uplift_at_30": uplift_at_k(y, w, score, 0.30, seed),
    }


def sanity_checks(y, w, score, seed: int = SEED) -> Mapping[str, bool]:
    """Bon phep tu kiem cua notebook — metric co do dung thu no noi khong.

    🚫 Metric uplift RAT de cai dat sai ma van cho ra so dep. Bon phep nay la
       cach notebook chung minh no khong bi vay; giu lai de moi lan doi metric
       deu phai chung minh lai.
    """
    rng = np.random.RandomState(seed)
    q_random = qini_score(y, w, rng.rand(len(y)), seed)
    q_model = qini_score(y, w, score, seed)
    q_oracle = qini_score(y, w, oracle_score(y, w), seed)
    noisy = np.asarray(score, dtype=float) + rng.normal(0, np.std(score) * 5, len(y))
    return {
        "xep_hang_ngau_nhien_qini_gan_0": abs(q_random) < 0.05,
        "biet_truoc_hon_han_ngau_nhien": q_oracle > q_random,
        "biet_truoc_bang_1": abs(q_oracle - 1.0) < 1e-9,
        "nhieu_lam_giam_qini": qini_score(y, w, noisy, seed) < q_model + 1e-9,
    }
