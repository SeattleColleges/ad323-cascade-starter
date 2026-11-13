# Checkpoints

If an earlier week didn't work out, you can start the next week from a
**checkpoint** instead: the course's working version of that week's output.
There is no penalty. Say which checkpoint you used in that week's `WEEK_NN.md`.

Checkpoints are published as releases of the course's checkpoints repo, after
each due date. Each release has its own README with exact instructions.

| Checkpoint | Contents | Put it here |
|---|---|---|
| checkpoint-02 | Bronze files + manifest (both sample months) | `checkpoints/checkpoint-02/` |
| checkpoint-03 | Silver Parquet + data-quality reports | `checkpoints/checkpoint-03/` |
| checkpoint-04 | Star-schema SQL + ERD | copied over `src/cascade/gold_sql/` |
| checkpoint-05 | dbt project with tests and the incremental fact | copied over `dbt_project/` |
| checkpoint-06 | Prefect flow | copied over `src/cascade/pipeline.py` |
| checkpoint-10 | Gold Parquet + CI files | `checkpoints/checkpoint-10/` |

**Commit the `checkpoints/` folder** so GitHub Actions can use it too. The
tests use these folders automatically:

- silver for Weeks 4-9 comes from `checkpoints/checkpoint-03/` if it is there;
- gold for Week 11 comes from `checkpoints/checkpoint-10/` if it is there;
- otherwise the tests run your own earlier stages.
