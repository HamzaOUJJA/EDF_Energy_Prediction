from spark.ml.constants import CANDIDATE_FEATURES, FORBIDDEN_LEAKAGE_FEATURES


def test_no_forbidden_features_in_candidate_features():
    leakage = set(CANDIDATE_FEATURES).intersection(FORBIDDEN_LEAKAGE_FEATURES)

    assert not leakage, (
        "Data leakage detected in CANDIDATE_FEATURES. "
        f"Forbidden features found: {sorted(leakage)}"
    )