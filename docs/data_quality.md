# Data Quality Rules

The pipeline writes a CSV quality report under `output/quality`.

| Rule | Target | Purpose |
| --- | --- | --- |
| `not_null` | `patient_id`, `gender`, `study_date` | Required field completeness |
| `unique` | `patient_id`, `study_date`, `exam_type` | Duplicate exam detection |
| `allowed_values` | `gender` | Basic domain validation |
| `date_parseable` | `study_date` | Date format validation |

The rules demonstrate a common production data development pattern: define checks, run them during orchestration, persist the result and decide whether failures should warn or block the downstream layer.
